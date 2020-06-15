#![allow(unused, dead_code)]

use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use crate::pb::{FlatUnixFs, PBLink, PBNode, ParsingFailed, UnixFsType};
use crate::file::{FileMetadata, FileReadFailed};
use crate::file::visit::{IdleFileVisit, FileVisit, Cache};
use crate::InvalidCidInLink;
use std::path::{Path, PathBuf};
use cid::Cid;

#[derive(Debug)]
pub struct Walker {
    current: InnerEntry,
    /// On the next call to `continue_walk` this will be the block, unless we have an ongoing file
    /// walk in which case we shortcircuit to continue it.
    next: Option<(Cid, String, usize)>,
    pending: Vec<(Cid, String, usize)>,
    // tried to recycle the names but that was consistently as fast and used more memory than just
    // cloning the strings
}

fn convert_link(
    depth: usize,
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
        Some(Cow::Owned(s)) => s,
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

fn convert_sharded_link(
    depth: usize,
    nth: usize,
    link: PBLink<'_>,
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let (depth, name) = match link.Name {
        Some(Cow::Borrowed(s)) if s.len() > 2 => (depth, s[2..].to_owned()),
        Some(Cow::Borrowed(s)) if s.len() == 2 => (depth - 1, String::from("")),
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => {
            if s.len() == 2 {
                (depth - 1, String::from(""))
            } else {
                assert!(s.len() > 2);
                (depth, s[2..].to_owned())
            }
        },
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

impl Walker {
    pub fn start<'a>(data: &'a [u8], cache: &mut Option<Cache>) -> Result<Walk<'a>, Error> {
        let flat = FlatUnixFs::try_from(data)?;
        let metadata = FileMetadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let inner = InnerEntry::new_root_dir(metadata);

                let links = flat.links
                    .into_iter()
                    .enumerate()
                    // 2 == number of ancestors this link needs to have on the path, this is after
                    // some trial and error so not entirely sure why ... ancestors always include
                    // the empty root in our case.
                    .map(|(nth, link)| convert_link(2, nth, link));

                Self::walk_directory(links, inner)
            },
            UnixFsType::HAMTShard => {
                let inner = InnerEntry::new_root_bucket(metadata);

                let links = flat.links
                    .into_iter()
                    .enumerate()
                    // 1 == ancestors should only be the [""]
                    .map(|(nth, link)| convert_sharded_link(1, nth, link));

                Self::walk_directory(links, inner)
            },
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, metadata, step) = IdleFileVisit::default()
                    .start_from_parsed(flat, cache)?;

                if let Some(visit) = step {
                    Ok(Walk::MultiBlockFile {
                        metadata,
                        visit
                    })
                } else {
                    Ok(Walk::SingleBlockFile {
                        bytes,
                        metadata
                    })
                }
            },
            UnixFsType::Metadata => todo!("metadata?"),
            UnixFsType::Symlink => todo!("?"),
        }
    }

    fn walk_directory<'a, I>(mut links: I, current: InnerEntry) -> Result<Walk<'a>, Error>
        where I: Iterator<Item = Result<(Cid, String, usize), InvalidCidInLink>> + 'a,
    {
        if let Some(next) = links.next() {
            let next = Some(next?);
            let pending = links.collect::<Result<Vec<_>, _>>()?;

            Ok(Walk::Walker(Walker {
                current,
                next,
                pending,
            }))
        } else {
            Ok(Walk::EmptyDirectory {
                metadata: current.into(),
            })
        }
    }

    pub fn as_entry<'a>(&'a self) -> Entry<'a> {
        self.current.as_entry()
    }

    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid>) {
        use InnerKind::*;
        let cids = self.pending.iter().map(|(cid, ..)| cid).rev();
        match self.current.kind {
            File(_, Some(ref visit)) => {
                let (first, rest) = visit.pending_links();
                (first, Either::Left(rest.chain(cids)))
            },
            _ => {
                let next = self.next.as_ref()
                    .expect("validated in start and continue_walk we have the next");
                (&next.0, Either::Right(cids))
            }
        }
    }

    pub fn continue_walk<'a>(mut self, bytes: &'a [u8], cache: &mut Option<Cache>) -> Result<ContinuedWalk<'a>, Error> {
        use InnerKind::*;

        match &mut self.current.kind {
            File(_, visit @ Some(_)) => {
                let (bytes, step) = visit.take()
                    .unwrap()
                    .continue_walk(bytes, cache)?;

                let file_continues = step.is_some();
                *visit = step;

                let segment = FileSegment::later(bytes, !file_continues);

                let state = if file_continues || self.next.is_some() {
                    State::Unfinished(self)
                } else {
                    State::Last(self.current)
                };

                return Ok(ContinuedWalk::File(segment, Item::from(state)))
            },
            _ => {}
        }

        let flat = FlatUnixFs::try_from(bytes)?;
        let metadata = FileMetadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let (cid, name, depth) = self.next.expect("continued without next");
                self.current.as_directory(
                    cid,
                    &name,
                    depth,
                    metadata,
                );

                // depth + 1 because all entries below a directory are children of next, as in,
                // deeper
                let mut links = flat.links
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
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))
            },
            UnixFsType::HAMTShard => {
                let (cid, name, depth) = self.next.expect("continued without next");
                self.current.as_bucket(cid, &name, depth);

                // similar to directory the depth is +1 for nested entries, but the sibling buckets
                // are at depth
                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(depth + 1, nth, link))
                    .rev();

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
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))

            },
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, metadata, step) = IdleFileVisit::default()
                    .start_from_parsed(flat, cache)?;

                let (cid, name, depth) = self.next.expect("continued without next");
                let file_continues = step.is_some();
                self.current.as_file(cid, &name, depth, metadata, step);

                let next = self.pending.pop();

                // FIXME: add test case for this being reversed and it's never the last
                let segment = FileSegment::first(bytes, !file_continues);

                let state = if file_continues || next.is_some() {
                    State::Unfinished(Self {
                        current: self.current,
                        next,
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::File(segment, Item::from(state)))
            },
            UnixFsType::Metadata => todo!("metadata?"),
            UnixFsType::Symlink => {
                let contents = flat.data.Data.as_deref().unwrap_or_default();

                let (cid, name, depth) = self.next.expect("continued without next");
                self.current.as_symlink(cid, &name, depth, metadata);

                let state = if let Some(next) = self.pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Symlink(bytes, Item::from(state)))
            },
        }
    }

    fn skip_current_file(mut self) -> Skipped {
        use InnerKind::*;
        match &mut self.current.kind {
            File(_, visit @ Some(_)) => {
                visit.take();

                if self.next.is_some() {
                    Skipped(State::Unfinished(self))
                } else {
                    Skipped(State::Last(self.current))
                }
            },
            Bucket(_) => todo!("we could skip shards as well by ... maybe?"),
            ref x => todo!("how to skip {:?}", x),
        }
    }

    // TODO: we could easily split a 'static value for a directory or bucket, which would pop all
    // entries at a single level out to do some parallel walking, though the skipping could already
    // be used to do that... Maybe we could return the filevisit on Skipped to save user from
    // re-creating one? How to do the same for directories?
}

enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> Iterator for Either<A, B>
    where A: Iterator,
          B: Iterator<Item = <A as Iterator>::Item>
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        use Either::*;
        match self {
            Left(a) => a.next(),
            Right(b) => b.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        use Either::*;
        match self {
            Left(a) => a.size_hint(),
            Right(b) => b.size_hint(),
        }
    }
}

#[derive(Debug)]
struct InnerEntry {
    kind: InnerKind,
    path: PathBuf,
    metadata: FileMetadata,
    depth: usize,
}

impl From<InnerEntry> for FileMetadata {
    fn from(e: InnerEntry) -> Self {
        e.metadata
    }
}

#[derive(Debug)]
enum InnerKind {
    RootDirectory,
    RootBucket,
    Bucket(Cid),
    Directory(Cid),
    File(Cid, Option<FileVisit>),
    Symlink(Cid),
}

#[derive(Debug)]
pub enum Entry<'a> {
    RootDirectory(&'a FileMetadata),
    Bucket(&'a Cid, &'a Path),
    Directory(&'a Cid, &'a Path, &'a FileMetadata),
    // TODO: add remaining bytes or something here?
    File(&'a Cid, &'a Path, &'a FileMetadata),
    Symlink(&'a Cid, &'a Path, &'a FileMetadata),
}

impl<'a> Entry<'a> {
    /// Returns the path for the latest entry.
    pub fn path(&self) -> &'a Path {
        use Entry::*;
        match self {
            RootDirectory(_) => "".as_ref(),
            Bucket(_, p)
            | Directory(_, p, _)
            | File(_, p, _)
            | Symlink(_, p, _) => p,
        }
    }
}

impl InnerEntry {
    fn new_root_dir(metadata: FileMetadata) -> Self {
        Self {
            kind: InnerKind::RootDirectory,
            path: PathBuf::new(),
            metadata,
            depth: 0,
        }
    }

    fn new_root_bucket(metadata: FileMetadata) -> Self {
        Self {
            kind: InnerKind::RootBucket,
            path: PathBuf::new(),
            metadata,
            depth: 0,
        }
    }

    pub fn as_entry<'a>(&'a self) -> Entry<'a> {
        use InnerKind::*;
        match &self.kind {
            RootDirectory => Entry::RootDirectory(&self.metadata),
            RootBucket => Entry::RootDirectory(&self.metadata),
            Bucket(cid) => Entry::Bucket(cid, &self.path),
            Directory(cid) => Entry::Directory(cid, &self.path, &self.metadata),
            File(cid, _) => Entry::File(cid, &self.path, &self.metadata),
            Symlink(cid) => Entry::Symlink(cid, &self.path, &self.metadata),
        }
    }

    fn set_path(&mut self, name: &str, depth: usize) {

        while self.depth >= depth && self.depth > 0 {
            assert!(self.path.pop());
            self.depth -= 1;
        }

        self.path.push(name);
        self.depth = depth;
    }

    fn as_directory(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | RootBucket  => {
                self.kind = Directory(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            Bucket(_)
            | Directory(_)
            | File(_, None)
            | Symlink(_) => {
                self.kind = Directory(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("dir after {:?}", x),
        }
    }

    fn as_bucket(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize
    ) {
        use InnerKind::*;
        match self.kind {
            RootBucket | Bucket(_) | File(_, None) | Symlink(_) => {
                self.kind = Bucket(cid);

                if name.is_empty() {
                    // continuation bucket going bucket -> bucket
                    while self.depth > depth {
                        assert!(self.path.pop());
                        self.depth -= 1;
                    }
                } else {
                    self.set_path(name, depth);
                }

                assert_eq!(self.depth, depth, "{:?}", self.path);
            },
            ref x => todo!("bucket after {:?}", x),
        }
    }

    fn as_file(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata,
        step: Option<FileVisit>,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | RootBucket => {
                assert_eq!(self.depth, 0);
                self.kind = File(cid, step);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            Bucket(_)
            | Directory(_)
            | File(_, None)
            | Symlink(_) => {
                self.kind = File(cid, step);
                self.set_path(name, depth);
                self.metadata = metadata;
            }
            ref x => todo!("file from {:?}", x),
        }
    }

    fn as_symlink(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory => {
                self.kind = Symlink(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            Bucket(_)
            | Directory(_)
            | File(_, None)
            | Symlink(_) => {
                self.kind = Symlink(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("symlink from {:?}", x),
        }
    }
}

#[derive(Debug)]
pub enum Walk<'a> {
    /// Walk was started on a file node which we don't know any name for
    SingleBlockFile {
        bytes: &'a [u8],
        metadata: FileMetadata,
    },
    Symlink {
        metadata: FileMetadata,
        target: &'a [u8],
    },
    /// Walk was started on a file which can be iterated further with the given FileVisit
    // TODO: not sure why not expose this through the continue_block as well? the path will just be
    // empty all the time.
    MultiBlockFile {
        metadata: FileMetadata,
        visit: FileVisit,
    },
    EmptyDirectory {
        metadata: FileMetadata,
    },
    /// Walk was started on a directory
    Walker(Walker),
}

// Wrapper to hide the state
#[derive(Debug)]
pub struct Item {
    state: State,
}

impl From<State> for Item {
    fn from(state: State) -> Self {
        Item {
            state
        }
    }
}

impl Item {
    // TODO: add path(&self) -> &Path

    pub fn as_entry(&self) -> Entry<'_> {
        match &self.state {
            State::Unfinished(w) => w.as_entry(),
            State::Last(w) => w.as_entry(),
        }
    }

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

#[derive(Debug)]
pub enum ContinuedWalk<'a> {
    File(FileSegment<'a>, Item),
    Directory(Item),
    Symlink(&'a [u8], Item),
}

#[derive(Debug)]
pub struct Skipped(State);

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

    /// Returns the total file size, remains constant during the walking of segments.
    pub fn total_file_size(&self) -> u64 {
        todo!("need to add this at filevisit level")
    }

    /// Returns true if this is the first block of the file, false otherwise.
    ///
    /// Note: First block can also be the last.
    pub fn is_first(&self) -> bool {
        self.first_block
    }

    /// Returns true if this is the last block of the file, false otherwise.
    ///
    /// Note: Last block can also be the first.
    pub fn is_last(&self) -> bool {
        self.last_block
    }
}

impl AsRef<[u8]> for FileSegment<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
pub enum Error {

}

impl From<ParsingFailed<'_>> for Error {
    fn from(e: ParsingFailed<'_>) -> Self {
        todo!()
    }
}

impl From<InvalidCidInLink> for Error {
    fn from(e: InvalidCidInLink) -> Self {
        todo!()
    }
}

impl From<FileReadFailed> for Error {
    fn from(e: FileReadFailed) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use crate::file::tests::FakeBlockstore;

    #[test]
    fn walk_two_file_directory() {
        let mut counts = walk_everything("QmPTotyhVnnfCu9R4qwR4cdhpi5ENaiP8ZJfdqsm8Dw2jB");

        assert_eq!(counts.remove(&PathBuf::from("")), Some(1));
        assert_eq!(counts.remove(&PathBuf::from("QmVkvLsSEm2uJx1h5Fqukje8mMPYg393o5C2kMCkF2bBTA")), Some(1));
        assert_eq!(counts.remove(&PathBuf::from("QmVkvLsSEm2uJx1h5Fqukje8mMPYg393o5C2kMCkF2bBTA/foobar.balanced")), Some(5));
        assert_eq!(counts.remove(&PathBuf::from("QmVkvLsSEm2uJx1h5Fqukje8mMPYg393o5C2kMCkF2bBTA/foobar.trickle")), Some(5));

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    #[test]
    fn sharded_dir() {
        use std::fmt::Write;

        // the hamt sharded directory is such that the root only has buckets so all of the actual files
        // are at second level buckets, each bucket should have 2 files. the actual files is in fact a single empty
        // file, linked from many names.
        let mut counts = walk_everything("QmZbFPTnDBMWbQ6iBxQAhuhLz8Nu9XptYS96e7cuf5wvbk");

        assert_eq!(counts.remove(&PathBuf::from("")), Some(9));
        let indices = [38, 48, 50, 58, 9, 33, 4, 34, 17, 37, 40, 16, 41, 3, 25, 49];
        let mut fmtbuf = String::new();
        let mut buf = PathBuf::from("");

        for i in &indices {
            fmtbuf.clear();
            write!(fmtbuf, "long-named-file-{:03}", i).unwrap();

            buf.clear();
            buf.push(&fmtbuf);
            assert_eq!(counts.remove(&buf), Some(1), "{:?}", buf);

        }

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    fn walk_everything(s: &str) -> HashMap<PathBuf, usize> {
        let mut ret = HashMap::new();

        let blocks = FakeBlockstore::with_fixtures();
        let block = blocks.get_by_str(s);
        let mut cache = None;

        let mut visit = match Walker::start(block, &mut cache).unwrap() {
            Walk::Walker(walker) => {
                *ret.entry(PathBuf::from(walker.as_entry().path())).or_insert(0) += 1;
                Some(walker)
            }
            x => unreachable!("{:?}", x),
        };

        while let Some(walker) = visit {
            let (next, _) = walker.pending_links();
            let block = blocks.get_by_cid(next);
            visit = match walker.continue_walk(block, &mut cache).unwrap() {
                ContinuedWalk::File(bytes, item) => {
                    *ret.entry(PathBuf::from(item.as_entry().path())).or_insert(0) += 1;
                    item.into_inner()
                },
                ContinuedWalk::Directory(item) => {
                    *ret.entry(PathBuf::from(item.as_entry().path())).or_insert(0) += 1;
                    item.into_inner()
                }
                x => unreachable!("{:?}", x),
            };
        }

        ret
    }
}
