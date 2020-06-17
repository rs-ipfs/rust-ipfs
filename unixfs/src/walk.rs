use crate::dir::{
    check_directory_supported, check_hamtshard_supported, ShardError, UnexpectedDirectoryProperties,
};
use crate::file::visit::{Cache, FileVisit, IdleFileVisit};
use crate::file::{FileError, FileReadFailed};
use crate::pb::{FlatUnixFs, PBLink, PBNode, ParsingFailed, UnixFsType};
use crate::{InvalidCidInLink, Metadata, UnexpectedNodeType};
use cid::Cid;
use either::Either;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::path::{Path, PathBuf};

/// `Walker` helps with walking a UnixFS tree, including all of the content and files. It is created with
/// `Walker::new` and walked over each block with `Walker::continue_block`. Use
/// `Walker::pending_links` to obtain the next [`Cid`] to be loaded and the prefetchable links.
#[derive(Debug)]
pub struct Walker {
    /// This is `None` until the first block has been visited. Any failing unwraps would be logic
    /// errors.
    current: Option<InnerEntry>,
    /// On the next call to `continue_walk` this will be the block, unless we have an ongoing file
    /// walk, in which case we short-circuit to continue it. Any failing unwraps of
    /// `self.next` would be logic errors.
    next: Option<(Cid, String, usize)>,
    pending: Vec<(Cid, String, usize)>,
    // tried to recycle the names but that was consistently as fast and used more memory than just
    // cloning the strings
}

/// Converts a link of specifically a Directory (and not a link of a HAMTShard).
fn convert_link(
    nested_depth: usize,
    nth: usize,
    link: PBLink<'_>,
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let name = match link.Name {
        Some(Cow::Borrowed(s)) if !s.is_empty() => s.to_owned(),
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => unreachable!("FlatUnixFs is never transformed to owned"),
    };
    assert!(!name.contains('/'));
    Ok((cid, name, nested_depth))
}

/// Converts a link of specifically a HAMTShard (and not a link of a Directory).
fn convert_sharded_link(
    nested_depth: usize,
    sibling_depth: usize,
    nth: usize,
    link: PBLink<'_>,
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let (depth, name) = match link.Name {
        Some(Cow::Borrowed(s)) if s.len() > 2 => (nested_depth, s[2..].to_owned()),
        Some(Cow::Borrowed(s)) if s.len() == 2 => (sibling_depth, String::from("")),
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => unreachable!("FlatUnixFs is never transformed to owned"),
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

impl Walker {
    /// Returns a new instance of a walker, ready to start from the given `Cid`.
    pub fn new(cid: Cid, root_name: String) -> Walker {
        // 1 == Path::ancestors().count() for an empty path
        let depth = if root_name.is_empty() { 1 } else { 2 };
        let next = Some((cid, root_name, depth));

        Walker {
            current: None,
            next,
            pending: Vec::new(),
        }
    }

    /// Returns a description of the kind of node the `Walker` is currently looking at, if
    /// `continue_walk` has been called after the value was created.
    pub fn as_entry(&'_ self) -> Option<Entry<'_>> {
        self.current.as_ref().map(|c| c.as_entry())
    }

    /// Returns the next `Cid` to load and pass its associated content to `continue_walk`.
    pub fn pending_links<'a>(&'a self) -> (&'a Cid, impl Iterator<Item = &'a Cid> + 'a) {
        use InnerKind::*;
        // rev: because we'll pop any of the pending
        let cids = self.pending.iter().map(|(cid, ..)| cid).rev();

        match self.current.as_ref().map(|c| &c.kind) {
            Some(File(_, Some(ref visit), _)) => {
                let (first, rest) = visit.pending_links();
                let next = self.next.iter().map(|(cid, _, _)| cid);
                (first, Either::Left(rest.chain(next.chain(cids))))
            }
            _ => {
                let next = self
                    .next
                    .as_ref()
                    .expect("we've validated that we have the next in new and continue_walk");
                (&next.0, Either::Right(cids))
            }
        }
    }

    /// Continues the walk.
    ///
    /// Returns a descriptor for the next element found as `ContinuedWalk` which includes the means
    /// to further continue the walk. `bytes` is the raw data of the next block, `cache` is an
    /// optional cache for data structures which can always be substituted with `&mut None`.
    pub fn continue_walk<'a>(
        mut self,
        bytes: &'a [u8],
        cache: &mut Option<Cache>,
    ) -> Result<ContinuedWalk<'a>, Error> {
        use InnerKind::*;

        if let Some(File(_, visit @ Some(_), _)) = self.current.as_mut().map(|c| &mut c.kind) {
            // we have an ongoing filevisit, the block must be related to it.
            let (bytes, step) = visit
                .take()
                .expect("matched visit was Some")
                .continue_walk(bytes, cache)?;

            let file_continues = step.is_some();
            *visit = step;

            let segment = FileSegment::later(bytes, !file_continues);

            let state = if file_continues || self.next.is_some() {
                State::Unfinished(self)
            } else {
                State::Last(self.current.unwrap())
            };

            return Ok(ContinuedWalk::File(segment, Item::from(state)));
        }

        let flat = FlatUnixFs::try_from(bytes)?;
        let metadata = Metadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let flat = crate::dir::check_directory_supported(flat)?;

                let (cid, name, depth) = self
                    .next
                    .expect("validated at new and earlier in this method");
                match self.current.as_mut() {
                    Some(current) => current.as_directory(cid, &name, depth, metadata),
                    _ => {
                        self.current = Some(InnerEntry::new_root_dir(cid, metadata, &name, depth));
                    }
                };

                // depth + 1 because all entries below a directory are children of next, as in,
                // deeper
                let mut links = flat
                    .links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_link(depth + 1, nth, link))
                    .rev();

                // replacing this with try_fold takes as many lines as the R: Try<Ok = B> cannot be
                // deduced without specifying the Error

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                let state = if let Some(next) = pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending,
                    })
                } else {
                    State::Last(self.current.unwrap())
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))
            }
            UnixFsType::HAMTShard => {
                let flat = crate::dir::check_hamtshard_supported(flat)?;

                // TODO: the first hamtshard must have metadata!
                let (cid, name, depth) = self.next.expect("validated at start and this method");

                match self.current.as_mut() {
                    Some(current) => {
                        if name.is_empty() {
                            // the name should be empty for all of the siblings
                            current.as_bucket(cid, &name, depth);
                        } else {
                            // but it should be non-empty for the directories
                            current.as_bucket_root(cid, &name, depth, metadata);
                        }
                    }
                    _ => {
                        self.current =
                            Some(InnerEntry::new_root_bucket(cid, metadata, &name, depth));
                    }
                }

                // similar to directory, the depth is +1 for nested entries, but the sibling buckets
                // are at depth
                let mut links = flat
                    .links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(depth + 1, depth, nth, link))
                    .rev();

                // TODO: it might be worthwhile to lose the `rev` and sort the pushed links using
                // the depth ascending. This should make sure we are first visiting the shortest
                // path items.

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                let state = if let Some(next) = pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending,
                    })
                } else {
                    State::Last(self.current.unwrap())
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))
            }
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, file_size, metadata, step) =
                    IdleFileVisit::default().start_from_parsed(flat, cache)?;

                let (cid, name, depth) = self
                    .next
                    .expect("validated at new and earlier in this method");
                let file_continues = step.is_some();

                match self.current.as_mut() {
                    Some(current) => current.as_file(cid, &name, depth, metadata, step, file_size),
                    _ => {
                        self.current = Some(InnerEntry::new_root_file(
                            cid, metadata, &name, step, file_size, depth,
                        ))
                    }
                }

                let next = self.pending.pop();
                let segment = FileSegment::first(bytes, !file_continues);

                let state = if file_continues || next.is_some() {
                    State::Unfinished(Self {
                        current: self.current,
                        next,
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current.unwrap())
                };

                Ok(ContinuedWalk::File(segment, Item::from(state)))
            }
            UnixFsType::Metadata => Err(Error::UnsupportedType(flat.data.Type.into())),
            UnixFsType::Symlink => {
                let contents = match flat.data.Data {
                    Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => bytes,
                    None | Some(Cow::Borrowed(_)) => &[][..],
                    _ => unreachable!("never used into_owned"),
                };

                let (cid, name, depth) = self.next.expect("continued without next");
                match self.current.as_mut() {
                    Some(current) => current.as_symlink(cid, &name, depth, metadata),
                    _ => {
                        self.current =
                            Some(InnerEntry::new_root_symlink(cid, metadata, &name, depth));
                    }
                }

                let state = if let next @ Some(_) = self.pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next,
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current.unwrap())
                };

                Ok(ContinuedWalk::Symlink(contents, Item::from(state)))
            }
        }
    }

    // TODO: we could easily split a 'static value for a directory or bucket, which would pop all
    // entries at a single level out to do some parallel walking, though the skipping could already
    // be used to do that... Maybe we could return the filevisit on Skipped to save user from
    // re-creating one? How to do the same for directories?
}

/// Represents what the `Walker` is currently looking at. Converted to `Entry` for public API.
#[derive(Debug)]
struct InnerEntry {
    kind: InnerKind,
    path: PathBuf,
    metadata: Metadata,
    depth: usize,
}

impl From<InnerEntry> for Metadata {
    fn from(e: InnerEntry) -> Self {
        e.metadata
    }
}

// FIXME: could simplify roots to optinal cid variants?
enum InnerKind {
    /// This is necessarily at the root of the walk
    RootDirectory(Cid),
    /// This is necessarily at the root of the walk
    BucketAtRoot(Cid),
    /// This is the metadata containing bucket, for which we have a name
    RootBucket(Cid),
    /// This is a sibling to a previous named metadata containing bucket
    Bucket(Cid),
    /// Directory on any level except root
    Directory(Cid),
    /// File optionally on the root level
    File(Cid, Option<FileVisit>, u64),
    /// Symlink optionally on the root level
    Symlink(Cid),
}

impl fmt::Debug for InnerKind {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use InnerKind::*;
        match self {
            RootDirectory(cid) => write!(fmt, "RootDirectory({})", cid),
            BucketAtRoot(cid) => write!(fmt, "BucketAtRoot({})", cid),
            RootBucket(cid) => write!(fmt, "RootBucket({})", cid),
            Bucket(cid) => write!(fmt, "Bucket({})", cid),
            Directory(cid) => write!(fmt, "Directory({})", cid),
            File(cid, _, sz) => write!(fmt, "File({}, _, {})", cid, sz),
            Symlink(cid) => write!(fmt, "Symlink({})", cid),
        }
    }
}

/// Representation of the current item of Walker or the last observed item.
#[derive(Debug)]
pub enum Entry<'a> {
    /// Current item is the root directory (HAMTShard or plain Directory).
    RootDirectory(&'a Cid, &'a Path, &'a Metadata),
    /// Current item is a continuation of a HAMTShard directory. Only the root HAMTShard will have
    /// file metadata.
    Bucket(&'a Cid, &'a Path),
    /// Current item is a non-root plain Directory or a HAMTShard directory.
    Directory(&'a Cid, &'a Path, &'a Metadata),
    /// Current item is possibly a root file with a path, metadata, and a total file size.
    File(&'a Cid, &'a Path, &'a Metadata, u64),
    /// Current item is possibly a root symlink.
    Symlink(&'a Cid, &'a Path, &'a Metadata),
}

impl<'a> Entry<'a> {
    /// Returns the path for the latest entry. This is created from a UTF-8 string and, as such, is always
    /// representable on all supported platforms.
    pub fn path(&self) -> &'a Path {
        use Entry::*;
        match self {
            RootDirectory(_, p, _)
            | Bucket(_, p)
            | Directory(_, p, _)
            | File(_, p, _, _)
            | Symlink(_, p, _) => p,
        }
    }

    /// Returns the metadata for the latest entry. It exists for initial directory entries, files,
    /// and symlinks but not for continued HamtShards.
    pub fn metadata(&self) -> Option<&'a Metadata> {
        use Entry::*;
        match self {
            Bucket(_, _) => None,
            RootDirectory(_, _, m) | Directory(_, _, m) | File(_, _, m, _) | Symlink(_, _, m) => {
                Some(m)
            }
        }
    }

    /// Returns the total size of the file this entry represents, or `None` if not a file.
    pub fn total_file_size(&self) -> Option<u64> {
        use Entry::*;
        match self {
            File(_, _, _, sz) => Some(*sz),
            _ => None,
        }
    }

    /// Returns the Cid for the latest entry.
    pub fn cid(&self) -> &Cid {
        use Entry::*;
        match self {
            RootDirectory(cid, _, _)
            | Bucket(cid, _)
            | Directory(cid, _, _)
            | File(cid, _, _, _)
            | Symlink(cid, _, _) => cid,
        }
    }
}

impl InnerEntry {
    fn new_root_dir(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::RootDirectory(cid),
            path,
            metadata,
            depth,
        }
    }

    fn new_root_bucket(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::BucketAtRoot(cid),
            path,
            metadata,
            depth,
        }
    }

    fn new_root_file(
        cid: Cid,
        metadata: Metadata,
        name: &str,
        step: Option<FileVisit>,
        file_size: u64,
        depth: usize,
    ) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::File(cid, step, file_size),
            path,
            metadata,
            depth,
        }
    }

    fn new_root_symlink(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::Symlink(cid),
            path,
            metadata,
            depth,
        }
    }

    pub fn as_entry(&self) -> Entry<'_> {
        use InnerKind::*;
        match &self.kind {
            RootDirectory(cid) | BucketAtRoot(cid) => {
                Entry::RootDirectory(cid, &self.path, &self.metadata)
            }
            RootBucket(cid) => Entry::Directory(cid, &self.path, &self.metadata),
            Bucket(cid) => Entry::Bucket(cid, &self.path),
            Directory(cid) => Entry::Directory(cid, &self.path, &self.metadata),
            File(cid, _, sz) => Entry::File(cid, &self.path, &self.metadata, *sz),
            Symlink(cid) => Entry::Symlink(cid, &self.path, &self.metadata),
        }
    }

    fn set_path(&mut self, name: &str, depth: usize) {
        debug_assert_eq!(self.depth, self.path.ancestors().count());

        while self.depth >= depth && self.depth > 0 {
            assert!(self.path.pop());
            self.depth = self
                .depth
                .checked_sub(1)
                .expect("undeflowed path components");
        }

        self.path.push(name);
        self.depth = depth;

        debug_assert_eq!(self.depth, self.path.ancestors().count());
    }

    fn as_directory(&mut self, cid: Cid, name: &str, depth: usize, metadata: Metadata) {
        use InnerKind::*;
        match self.kind {
            RootDirectory(_)
            | BucketAtRoot(_)
            | Bucket(_)
            | RootBucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = Directory(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!("directory ({}, {}, {}) following {:?}", cid, name, depth, x),
        }
    }

    fn as_bucket_root(&mut self, cid: Cid, name: &str, depth: usize, metadata: Metadata) {
        use InnerKind::*;
        match self.kind {
            RootDirectory(_)
            | BucketAtRoot(_)
            | Bucket(_)
            | RootBucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = RootBucket(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!(
                "root bucket ({}, {}, {}) following {:?}",
                cid, name, depth, x
            ),
        }
    }

    fn as_bucket(&mut self, cid: Cid, name: &str, depth: usize) {
        use InnerKind::*;
        match self.kind {
            BucketAtRoot(_) => {
                assert_eq!(self.depth, depth, "{:?}", self.path);
            }
            RootBucket(_) | Bucket(_) | File(_, None, _) | Symlink(_) => {
                self.kind = Bucket(cid);

                assert!(name.is_empty());
                // continuation bucket going bucket -> bucket
                while self.depth > depth {
                    assert!(self.path.pop());
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .expect("underlowed depth calculation during bucket->bucket");
                }

                assert_eq!(self.depth, depth, "{:?}", self.path);
            }
            ref x => unreachable!("bucket ({}, {}, {}) following {:?}", cid, name, depth, x),
        }
    }

    fn as_file(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: Metadata,
        step: Option<FileVisit>,
        file_size: u64,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory(_)
            | BucketAtRoot(_)
            | RootBucket(_)
            | Bucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = File(cid, step, file_size);
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!(
                "file ({}, {}, {}, {}) following {:?}",
                cid, name, depth, file_size, x
            ),
        }
    }

    fn as_symlink(&mut self, cid: Cid, name: &str, depth: usize, metadata: Metadata) {
        use InnerKind::*;
        match self.kind {
            RootDirectory(_)
            | BucketAtRoot(_)
            | RootBucket(_)
            | Bucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = Symlink(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!("symlink ({}, {}, {}) following {:?}", cid, name, depth, x),
        }
    }
}

/// Structure to hide the internal `Walker` state and to provide an optional way to continue when
/// there are links left to continue with `Item::into_inner()`.
#[derive(Debug)]
pub struct Item {
    state: State,
}

impl From<State> for Item {
    fn from(state: State) -> Self {
        Item { state }
    }
}

impl Item {
    /// Returns either the representation of the tree node where the walk last continued to, or the last
    /// loaded block.
    pub fn as_entry(&self) -> Entry<'_> {
        match &self.state {
            State::Unfinished(w) => w
                .as_entry()
                .expect("if the walk is unfinished, there has to be an entry"),
            State::Last(w) => w.as_entry(),
        }
    }

    /// Returns `Some` when the walk can be continued, or `None` if all links have been exhausted.
    pub fn into_inner(self) -> Option<Walker> {
        match self.state {
            State::Unfinished(w) => Some(w),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum State {
    Unfinished(Walker),
    Last(InnerEntry),
}

/// Representation of the walk progress. The common `Item` can be used to continue the walk.
#[derive(Debug)]
pub enum ContinuedWalk<'a> {
    /// Currently looking at a file. The first tuple value contains the file bytes accessible
    /// from the block, which can also be an empty slice.
    File(FileSegment<'a>, Item),
    /// Currently looking at a directory.
    Directory(Item),
    /// Currently looking at a symlink. The first tuple value contains the symlink target path. It
    /// might be convertible to UTF-8, but this is not specified in the spec.
    Symlink(&'a [u8], Item),
}

impl ContinuedWalk<'_> {
    /// Returns the `Item` describing the current entry; helpful when only listing the tree.
    pub fn into_inner(self) -> Item {
        use ContinuedWalk::*;
        match self {
            File(_, item) | Directory(item) | Symlink(_, item) => item,
        }
    }
}

/// A slice of bytes of a possibly multi-block file. The slice can be accessed via `as_bytes()` or
/// `AsRef<[u8]>::as_ref()`.
#[derive(Debug)]
pub struct FileSegment<'a> {
    bytes: &'a [u8],
    first_block: bool,
    last_block: bool,
}

impl<'a> FileSegment<'a> {
    fn first(bytes: &'a [u8], last_block: bool) -> Self {
        FileSegment {
            bytes,
            first_block: true,
            last_block,
        }
    }

    fn later(bytes: &'a [u8], last_block: bool) -> Self {
        FileSegment {
            bytes,
            first_block: false,
            last_block,
        }
    }

    /// Returns `true` if this is the first block in the file, `false` otherwise.
    ///
    /// Note: the first block can also be the last one.
    pub fn is_first(&self) -> bool {
        self.first_block
    }

    /// Returns `true` if this is the last block in the file, `false` otherwise.
    ///
    /// Note: the last block can also be the first one.
    pub fn is_last(&self) -> bool {
        self.last_block
    }

    /// Returns a slice into the file's bytes, which can be empty, as is the case for any
    /// intermediate blocks which only contain links to further blocks.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl AsRef<[u8]> for FileSegment<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Errors which can occur while walking a tree.
#[derive(Debug)]
pub enum Error {
    /// An unsupported type of UnixFS node was encountered. There should be a way to skip these. Of the
    /// defined types only `Metadata` is unsupported, all undefined types as of 2020-06 are also
    /// unsupported.
    UnsupportedType(UnexpectedNodeType),

    /// This error is returned when a file e.g. links to a non-Raw or non-File subtree.
    UnexpectedType(UnexpectedNodeType),

    /// dag-pb node parsing failed, perhaps the block is not a dag-pb node?
    DagPbParsingFailed(quick_protobuf::Error),

    /// Failed to parse the unixfs node inside the dag-pb node.
    UnixFsParsingFailed(quick_protobuf::Error),

    /// dag-pb node contained no data.
    EmptyDagPbNode,

    /// dag-pb link could not be converted to a Cid
    InvalidCid(InvalidCidInLink),

    /// A File has an invalid structure
    File(FileError),

    /// A Directory has an unsupported structure
    UnsupportedDirectory(UnexpectedDirectoryProperties),

    /// HAMTSharded directory has unsupported properties
    UnsupportedHAMTShard(ShardError),
}

impl From<ParsingFailed<'_>> for Error {
    fn from(e: ParsingFailed<'_>) -> Self {
        use ParsingFailed::*;
        match e {
            InvalidDagPb(e) => Error::DagPbParsingFailed(e),
            InvalidUnixFs(e, _) => Error::UnixFsParsingFailed(e),
            NoData(_) => Error::EmptyDagPbNode,
        }
    }
}

impl From<InvalidCidInLink> for Error {
    fn from(e: InvalidCidInLink) -> Self {
        Error::InvalidCid(e)
    }
}

impl From<FileReadFailed> for Error {
    fn from(e: FileReadFailed) -> Self {
        use FileReadFailed::*;
        match e {
            File(e) => todo!(),
            UnexpectedType(ut) => Error::UnexpectedType(ut),
            Read(_) => unreachable!("FileVisit does not parse any blocks"),
            InvalidCid(l) => Error::InvalidCid(l),
        }
    }
}

impl From<UnexpectedDirectoryProperties> for Error {
    fn from(e: UnexpectedDirectoryProperties) -> Self {
        Error::UnsupportedDirectory(e)
    }
}

impl From<ShardError> for Error {
    fn from(e: ShardError) -> Self {
        Error::UnsupportedHAMTShard(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;

        match self {
            UnsupportedType(ut) => write!(fmt, "unsupported UnixFs type: {:?}", ut),
            UnexpectedType(ut) => write!(fmt, "link to unexpected UnixFs type from File: {:?}", ut),
            DagPbParsingFailed(e) => write!(fmt, "failed to parse the outer dag-pb: {}", e),
            UnixFsParsingFailed(e) => write!(fmt, "failed to parse the inner UnixFs: {}", e),
            EmptyDagPbNode => write!(fmt, "failed to parse the inner UnixFs: no data"),
            InvalidCid(e) => write!(fmt, "link contained an invalid Cid: {}", e),
            File(e) => write!(fmt, "invalid file: {}", e),
            UnsupportedDirectory(udp) => write!(fmt, "unsupported directory: {}", udp),
            UnsupportedHAMTShard(se) => write!(fmt, "unsupported hamtshard: {}", se),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FakeBlockstore;
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[test]
    fn walk_two_file_directory_empty() {
        two_file_directory_scenario("");
    }

    #[test]
    fn walk_two_file_directory_named() {
        two_file_directory_scenario("foo");
    }

    fn two_file_directory_scenario(root_name: &str) {
        println!("new two_file_directory_scenario");
        let mut counts =
            walk_everything(root_name, "QmPTotyhVnnfCu9R4qwR4cdhpi5ENaiP8ZJfdqsm8Dw2jB");

        let mut pb = PathBuf::new();
        pb.push(root_name);
        counts.checked_removal(&pb, 1);

        pb.push("QmVkvLsSEm2uJx1h5Fqukje8mMPYg393o5C2kMCkF2bBTA");
        counts.checked_removal(&pb, 1);

        pb.push("foobar.balanced");
        counts.checked_removal(&pb, 5);

        assert!(pb.pop());
        pb.push("foobar.trickle");
        counts.checked_removal(&pb, 5);

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    #[test]
    fn sharded_dir_different_root_empty() {
        sharded_dir_scenario("");
    }

    #[test]
    fn sharded_dir_different_root_named() {
        sharded_dir_scenario("foo");
    }

    fn sharded_dir_scenario(root_name: &str) {
        use std::fmt::Write;

        // the hamt sharded directory is such that the root only has buckets so all of the actual files
        // are at second level buckets, each bucket should have 2 files. the actual files, in fact, constitute a single empty
        // file, linked from many names.

        let mut counts =
            walk_everything(root_name, "QmZbFPTnDBMWbQ6iBxQAhuhLz8Nu9XptYS96e7cuf5wvbk");
        let mut buf = PathBuf::from(root_name);

        counts.checked_removal(&buf, 9);

        let indices = [38, 48, 50, 58, 9, 33, 4, 34, 17, 37, 40, 16, 41, 3, 25, 49];
        let mut fmtbuf = String::new();

        for (index, i) in indices.iter().enumerate() {
            fmtbuf.clear();
            write!(fmtbuf, "long-named-file-{:03}", i).unwrap();

            if index > 0 {
                buf.pop();
            }
            buf.push(&fmtbuf);

            counts.checked_removal(&buf, 1);
        }

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    #[test]
    fn top_level_single_block_file_empty() {
        single_block_top_level_file_scenario("");
    }

    #[test]
    fn top_level_single_block_file_named() {
        single_block_top_level_file_scenario("empty.txt");
    }

    fn single_block_top_level_file_scenario(root_name: &str) {
        let mut counts =
            walk_everything(root_name, "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 1);
    }

    #[test]
    fn top_level_symlink_empty() {
        top_level_symlink_scenario("");
    }

    #[test]
    fn top_level_symlink_named() {
        top_level_symlink_scenario("this_links_to_foobar");
    }

    fn top_level_symlink_scenario(root_name: &str) {
        let mut counts =
            walk_everything(root_name, "QmNgQEdXVdLw79nH2bnxLMxnyWMaXrijfqMTiDVat3iyuz");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 1);
    }

    #[test]
    fn top_level_multiblock_file_empty() {
        top_level_multiblock_file_scenario("");
    }

    #[test]
    fn top_level_multiblock_file_named() {
        top_level_multiblock_file_scenario("foobar_and_newline.txt");
    }

    fn top_level_multiblock_file_scenario(root_name: &str) {
        let mut counts =
            walk_everything(root_name, "QmWfQ48ChJUj4vWKFsUDe4646xCBmXgdmNfhjz9T7crywd");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 5);
    }

    #[test]
    fn test_walked_file_segments() {
        let blocks = FakeBlockstore::with_fixtures();

        let trickle_foobar =
            cid::Cid::try_from("QmWfQ48ChJUj4vWKFsUDe4646xCBmXgdmNfhjz9T7crywd").unwrap();
        let mut visit = Some(Walker::new(trickle_foobar, String::new()));

        let mut counter = 0;

        while let Some(walker) = visit {
            let (next, _) = walker.pending_links();

            let block = blocks.get_by_cid(&next);

            counter += 1;

            visit = match walker.continue_walk(&block, &mut None).unwrap() {
                ContinuedWalk::File(segment, item) => {
                    match counter {
                        1 => {
                            // the root block has only links
                            assert!(segment.as_ref().is_empty());
                            assert!(segment.is_first());
                            assert!(!segment.is_last());
                        }
                        2..=4 => {
                            assert_eq!(segment.as_ref().len(), 2);
                            assert!(!segment.is_first());
                            assert!(!segment.is_last());
                        }
                        5 => {
                            assert_eq!(segment.as_ref().len(), 1);
                            assert!(!segment.is_first());
                            assert!(segment.is_last());
                        }
                        _ => unreachable!(),
                    }

                    item.into_inner()
                }
                x => unreachable!("{:?}", x),
            };
        }
    }

    trait CountsExt {
        fn checked_removal(&mut self, key: &PathBuf, expected: usize);
    }

    impl CountsExt for HashMap<PathBuf, usize> {
        fn checked_removal(&mut self, key: &PathBuf, expected: usize) {
            use std::collections::hash_map::Entry::*;

            match self.entry(key.clone()) {
                Occupied(oe) => {
                    assert_eq!(oe.remove(), expected);
                }
                Vacant(_) => {
                    panic!(
                        "no such key {:?} (expected {}) in {:#?}",
                        key, expected, self
                    );
                }
            }
        }
    }

    fn walk_everything(root_name: &str, cid: &str) -> HashMap<PathBuf, usize> {
        let mut ret = HashMap::new();

        let blocks = FakeBlockstore::with_fixtures();

        let mut cache = None;
        let mut visit = Some(Walker::new(
            cid::Cid::try_from(cid).unwrap(),
            root_name.to_string(),
        ));

        while let Some(walker) = visit {
            let (next, _) = walker.pending_links();
            let block = blocks.get_by_cid(next);
            let item = walker
                .continue_walk(block, &mut cache)
                .unwrap()
                .into_inner();
            *ret.entry(PathBuf::from(item.as_entry().path()))
                .or_insert(0) += 1;
            visit = item.into_inner();
        }

        ret
    }
}
