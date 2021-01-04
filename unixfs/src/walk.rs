use crate::dir::{ShardError, UnexpectedDirectoryProperties};
use crate::file::visit::{Cache, FileVisit, IdleFileVisit};
use crate::file::{FileError, FileReadFailed};
use crate::pb::{FlatUnixFs, PBLink, ParsingFailed, UnixFsType};
use crate::{InvalidCidInLink, Metadata, UnexpectedNodeType};
use alloc::borrow::Cow;
use cid::Cid;
use core::convert::TryFrom;
use core::fmt;
use either::Either;
use std::path::{Path, PathBuf};

/// `Walker` helps with walking a UnixFS tree, including all of the content and files. It is
/// created with `Walker::new` and walked over each block with `Walker::continue_block`. Use
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
    should_continue: bool,
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
        Some(Cow::Owned(_s)) => unreachable!("FlatUnixFs is never transformed to owned"),
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
        Some(Cow::Owned(_s)) => unreachable!("FlatUnixFs is never transformed to owned"),
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
            should_continue: true,
        }
    }

    /// Returns the next [`Cid`] to load and pass its associated content to [`next`].
    ///
    /// # Panics
    ///
    /// When [`should_continue()`] returns `false`.
    // TODO: perhaps this should return an option?
    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid> + '_) {
        use InnerKind::*;
        // rev: because we'll pop any of the pending
        let cids = self.pending.iter().map(|(cid, ..)| cid).rev();

        match self.current.as_ref().map(|c| &c.kind) {
            Some(File(Some(ref visit), _)) => {
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
    pub fn next<'a: 'c, 'b: 'c, 'c>(
        &'a mut self,
        bytes: &'b [u8],
        cache: &mut Option<Cache>,
    ) -> Result<ContinuedWalk<'c>, Error> {
        let Self {
            current,
            next,
            pending,
            should_continue,
        } = self;

        *should_continue = false;

        if let Some(InnerEntry {
            cid,
            kind: InnerKind::File(visit @ Some(_), sz),
            metadata,
            path,
            ..
        }) = current
        {
            // we have an ongoing filevisit, the block must be related to it.
            let (bytes, step) = visit.take().unwrap().continue_walk(bytes, cache)?;
            let file_continues = step.is_some();
            let segment = FileSegment::later(bytes, !file_continues);
            *visit = step;
            if file_continues || next.is_some() {
                *should_continue = true;
            }
            return Ok(ContinuedWalk::File(segment, cid, path, metadata, *sz));
        }

        let flat = FlatUnixFs::try_from(bytes)?;
        let metadata = Metadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let flat = crate::dir::check_directory_supported(flat)?;
                let (cid, name, depth) = next.take().expect("validated at new and earlier");

                // depth + 1 because all entries below a directory are children of next, as in,
                // deeper
                let links = flat
                    .links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_link(depth + 1, nth, link))
                    .rev();

                // replacing this with try_fold takes as many lines as the R: Try<Ok = B> cannot be
                // deduced without specifying the Error
                for link in links {
                    pending.push(link?);
                }

                if let next_local @ Some(_) = pending.pop() {
                    *next = next_local;
                    *should_continue = true;
                }

                Ok(match current {
                    None => {
                        *current = Some(InnerEntry::new_root_dir(cid, metadata, &name, depth));
                        let ie = current.as_ref().unwrap();
                        ContinuedWalk::RootDirectory(&ie.cid, &ie.path, &ie.metadata)
                    }
                    Some(ie) => {
                        ie.as_directory(cid, &name, depth, metadata);
                        ContinuedWalk::Directory(&ie.cid, &ie.path, &ie.metadata)
                    }
                })
            }
            UnixFsType::HAMTShard => {
                let flat = crate::dir::check_hamtshard_supported(flat)?;
                let (cid, name, depth) = next.take().expect("validated at start and this method");

                // similar to directory, the depth is +1 for nested entries, but the sibling buckets
                // are at depth
                let links = flat
                    .links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(depth + 1, depth, nth, link))
                    .rev();

                // TODO: it might be worthwhile to lose the `rev` and sort the pushed links using
                // the depth ascending. This should make sure we are first visiting the shortest
                // path items.
                for link in links {
                    pending.push(link?);
                }

                if let next_local @ Some(_) = pending.pop() {
                    *next = next_local;
                    *should_continue = true;
                }

                Ok(match current {
                    None => {
                        *current = Some(InnerEntry::new_root_bucket(cid, metadata, &name, depth));
                        let ie = current.as_ref().unwrap();
                        ContinuedWalk::RootDirectory(&ie.cid, &ie.path, &ie.metadata)
                    }
                    Some(ie) => {
                        // the name should be empty for all of the siblings
                        if name.is_empty() {
                            ie.as_bucket(cid, &name, depth);
                            ContinuedWalk::Bucket(&ie.cid, &ie.path)
                        }
                        // but it should be non-empty for the directories
                        else {
                            ie.as_bucket_root(cid, &name, depth, metadata);
                            ContinuedWalk::RootDirectory(&ie.cid, &ie.path, &ie.metadata)
                        }
                    }
                })
            }
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, file_size, metadata, step) =
                    IdleFileVisit::default().start_from_parsed(flat, cache)?;
                let (cid, name, depth) = next.take().expect("validated at new and earlier");
                let file_continues = step.is_some();

                match current {
                    None => {
                        let ie =
                            InnerEntry::new_root_file(cid, metadata, &name, step, file_size, depth);
                        *current = Some(ie);
                    }
                    Some(ie) => {
                        ie.as_file(cid, &name, depth, metadata, step, file_size);
                    }
                };

                let next_local = pending.pop();
                if file_continues || next_local.is_some() {
                    *next = next_local;
                    *should_continue = true;
                }

                let segment = FileSegment::first(bytes, !file_continues);

                let ie = current.as_ref().unwrap();
                Ok(ContinuedWalk::File(
                    segment,
                    &ie.cid,
                    &ie.path,
                    &ie.metadata,
                    file_size,
                ))
            }
            UnixFsType::Metadata => Err(Error::UnsupportedType(flat.data.Type.into())),
            UnixFsType::Symlink => {
                let contents = match flat.data.Data {
                    Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => bytes,
                    None | Some(Cow::Borrowed(_)) => &[][..],
                    _ => unreachable!("never used into_owned"),
                };

                let (cid, name, depth) = next.take().expect("continued without next");
                match current {
                    None => {
                        let ie = InnerEntry::new_root_symlink(cid, metadata, &name, depth);
                        *current = Some(ie);
                    }
                    Some(ie) => {
                        ie.as_symlink(cid, &name, depth, metadata);
                    }
                };

                if let next_local @ Some(_) = pending.pop() {
                    *next = next_local;
                    *should_continue = true;
                }

                let ie = current.as_ref().unwrap();
                Ok(ContinuedWalk::Symlink(
                    contents,
                    &ie.cid,
                    &ie.path,
                    &ie.metadata,
                ))
            }
        }
    }

    /// Returns `true` if there are more links to walk over.
    pub fn should_continue(&self) -> bool {
        self.should_continue
    }

    // TODO: we could easily split a 'static value for a directory or bucket, which would pop all
    // entries at a single level out to do some parallel walking, though the skipping could already
    // be used to do that... Maybe we could return the filevisit on Skipped to save user from
    // re-creating one? How to do the same for directories?
}

/// Represents what the `Walker` is currently looking at.
struct InnerEntry {
    cid: Cid,
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

#[derive(Debug)]
enum InnerKind {
    /// This is necessarily at the root of the walk
    RootDirectory,
    /// This is necessarily at the root of the walk
    BucketAtRoot,
    /// This is the metadata containing bucket, for which we have a name
    RootBucket,
    /// This is a sibling to a previous named metadata containing bucket
    Bucket,
    /// Directory on any level except root
    Directory,
    /// File optionally on the root level
    File(Option<FileVisit>, u64),
    /// Symlink optionally on the root level
    Symlink,
}

impl InnerEntry {
    fn new_root_dir(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            cid,
            kind: InnerKind::RootDirectory,
            path,
            metadata,
            depth,
        }
    }

    fn new_root_bucket(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            cid,
            kind: InnerKind::BucketAtRoot,
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
            cid,
            kind: InnerKind::File(step, file_size),
            path,
            metadata,
            depth,
        }
    }

    fn new_root_symlink(cid: Cid, metadata: Metadata, name: &str, depth: usize) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            cid,
            kind: InnerKind::Symlink,
            path,
            metadata,
            depth,
        }
    }

    fn set_path(&mut self, name: &str, depth: usize) {
        debug_assert_eq!(self.depth, self.path.ancestors().count());

        while self.depth >= depth && self.depth > 0 {
            assert!(self.path.pop());
            self.depth = self
                .depth
                .checked_sub(1)
                .expect("underflowed path components");
        }

        self.path.push(name);
        self.depth = depth;

        debug_assert_eq!(self.depth, self.path.ancestors().count());
    }

    fn as_directory(&mut self, cid: Cid, name: &str, depth: usize, metadata: Metadata) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | Bucket
            | RootBucket
            | Directory
            | File(None, _)
            | Symlink => {
                self.cid = cid;
                self.kind = Directory;
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!("directory ({}, {}, {}) following {:?}", cid, name, depth, x),
        }
    }

    fn as_bucket_root(&mut self, cid: Cid, name: &str, depth: usize, metadata: Metadata) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | Bucket
            | RootBucket
            | Directory
            | File(None, _)
            | Symlink => {
                self.cid = cid;
                self.kind = RootBucket;
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
            BucketAtRoot => {
                assert_eq!(self.depth, depth, "{:?}", self.path);
            }
            RootBucket | Bucket | File(None, _) | Symlink => {
                self.cid = cid;
                self.kind = Bucket;

                assert!(name.is_empty());
                // continuation bucket going bucket -> bucket
                while self.depth > depth {
                    assert!(self.path.pop());
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .expect("underflowed depth calculation during bucket->bucket");
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
            RootDirectory
            | BucketAtRoot
            | RootBucket
            | Bucket
            | Directory
            | File(None, _)
            | Symlink => {
                self.cid = cid;
                self.kind = File(step, file_size);
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
            Bucket
            | BucketAtRoot
            | Directory
            | File(None, _)
            | RootBucket
            | RootDirectory
            | Symlink => {
                self.cid = cid;
                self.kind = Symlink;
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => unreachable!("symlink ({}, {}, {}) following {:?}", cid, name, depth, x),
        }
    }
}

impl fmt::Debug for InnerEntry {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("InnerEntry")
            .field("depth", &self.depth)
            .field("kind", &self.kind)
            .field("cid", &format_args!("{}", self.cid))
            .field("path", &self.path)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Representation of the walk progress.
#[derive(Debug)]
pub enum ContinuedWalk<'a> {
    /// Currently looking at a continuation of a HAMT sharded directory. Usually safe to ignore.
    Bucket(&'a Cid, &'a Path),
    /// Currently looking at a directory.
    Directory(&'a Cid, &'a Path, &'a Metadata),
    /// Currently looking at a file. The first tuple value contains the file bytes accessible
    /// from the block, which can also be an empty slice.
    File(FileSegment<'a>, &'a Cid, &'a Path, &'a Metadata, u64),
    /// Currently looking at a root directory.
    RootDirectory(&'a Cid, &'a Path, &'a Metadata),
    /// Currently looking at a symlink. The first tuple value contains the symlink target path. It
    /// might be convertible to UTF-8, but this is not specified in the spec.
    Symlink(&'a [u8], &'a Cid, &'a Path, &'a Metadata),
}

impl ContinuedWalk<'_> {
    #[cfg(test)]
    fn path(&self) -> &Path {
        match self {
            Self::Bucket(_, p)
            | Self::Directory(_, p, ..)
            | Self::File(_, _, p, ..)
            | Self::RootDirectory(_, p, ..)
            | Self::Symlink(_, _, p, ..) => p,
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
            File(e) => Error::File(e),
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
        use core::fmt::Write;

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
        let buf = PathBuf::from(root_name);
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
        let buf = PathBuf::from(root_name);
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
        let buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 5);
    }

    #[test]
    fn test_walked_file_segments() {
        let blocks = FakeBlockstore::with_fixtures();

        let trickle_foobar =
            cid::Cid::try_from("QmWfQ48ChJUj4vWKFsUDe4646xCBmXgdmNfhjz9T7crywd").unwrap();
        let mut walker = Walker::new(trickle_foobar, String::new());

        let mut counter = 0;

        while walker.should_continue() {
            let (next, _) = walker.pending_links();

            let block = blocks.get_by_cid(&next);

            counter += 1;

            match walker.next(&block, &mut None).unwrap() {
                ContinuedWalk::File(segment, ..) => {
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
        let mut walker = Walker::new(cid::Cid::try_from(cid).unwrap(), root_name.to_string());

        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let block = blocks.get_by_cid(next);
            let cw = walker.next(block, &mut cache).unwrap();
            *ret.entry(PathBuf::from(cw.path())).or_insert(0) += 1;
        }

        ret
    }
}
