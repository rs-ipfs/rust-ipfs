#![allow(unused, dead_code)]

use std::borrow::Cow;
use std::convert::TryFrom;
use crate::pb::{FlatUnixFs, PBLink, PBNode, ParsingFailed, UnixFsType};
use crate::file::{FileMetadata, FileReadFailed};
use crate::file::visit::{IdleFileVisit, FileVisit, Cache};
use crate::InvalidCidInLink;
use std::path::{Path, PathBuf};
use cid::Cid;

#[derive(Debug)]
pub struct Walker {
    current: InnerEntry,
    /// On the next call to `continue_walk` this will be the block, unless we have an ongoing file,
    /// walk in which case we shortcircuit to continue it.
    next: Option<(Cid, String, usize)>,
    pending: Vec<(Cid, String, usize)>,
    path_recycler: Vec<String>,
}

fn convert_link(
    depth: usize,
    nth: usize,
    link: PBLink<'_>,
    recycler: &mut Vec<String>
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let name = match link.Name {
        Some(Cow::Borrowed(s)) if !s.is_empty() => {
            if let Some(mut o) = recycler.pop() {
                o.clear();
                o.push_str(s);
                o
            } else {
                s.to_string()
            }
        }
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
    recycler: &mut Vec<String>
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let (depth, name) = match link.Name {
        Some(Cow::Borrowed(s)) if s.len() > 2 => {
            let mut o = recycler.pop().unwrap_or_default();
            o.clear();
            o.push_str(&s[2..]);
            (depth, o)
        },
        Some(Cow::Borrowed(s)) if s.len() == 2 => {
            (depth - 1, String::from(""))
        }
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => {
            if s.len() == 2 {
                (depth - 1, String::from(""))
            } else {
                assert!(s.len() > 2);
                let mut o = recycler.pop().unwrap_or_default();
                o.clear();
                o.push_str(&s[2..]);
                recycler.push(s);
                (depth, o)
            }
        },
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

impl Walker {
    pub fn start<'a>(data: &'a [u8], cache: &mut Option<Cache>) -> Result<Walk<'a>, Error> {
        let flat = FlatUnixFs::try_from(data)?;

        match flat.data.Type {
            UnixFsType::Directory => {
                let mut path_recycler = Vec::new();

                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_link(2, nth, link, &mut path_recycler));

                let next = links.next();

                if let Some(next) = next {
                    let next = next?;
                    let pending = links.collect::<Result<Vec<_>, _>>()?;
                    let current = InnerEntry::new_root_dir(FileMetadata::from(&flat.data));

                    Ok(Walk::Walker(Walker {
                        current,
                        next: Some(next),
                        pending,
                        path_recycler,
                    }))
                } else {
                    todo!("empty root directory")
                }
            },
            UnixFsType::HAMTShard => {
                let mut path_recycler = Vec::new();

                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(1, nth, link, &mut path_recycler));

                let next = links.next();

                if let Some(next) = next {
                    let next = next?;
                    let pending = links.collect::<Result<Vec<_>, _>>()?;
                    let current = InnerEntry::new_root_bucket(FileMetadata::from(&flat.data));

                    Ok(Walk::Walker(Walker {
                        current,
                        next: Some(next),
                        pending,
                        path_recycler,
                    }))
                } else {
                    todo!("empty root directory")
                }

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
                (&self.next.as_ref().unwrap().0, Either::Right(cids))
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

                let was_some = step.is_some();
                *visit = step;

                let segment = if was_some {
                    FileSegment::NotLast(bytes)
                } else {
                    FileSegment::Last(bytes)
                };

                if was_some || self.next.is_some() {
                    return Ok(ContinuedWalk::File(segment, Item { state: State::Unfinished(self) }));
                } else {
                    return Ok(ContinuedWalk::File(segment, Item { state: State::Last(self.current) }));
                }
            },
            _ => {}
        }

        let flat = FlatUnixFs::try_from(bytes)?;

        match flat.data.Type {
            UnixFsType::Directory => {

                let mut path_recycler = self.path_recycler;
                let (cid, name, depth) = self.next.expect("continued without next");
                //println!("continued over to Directory {}, {:?}, {}", cid, name, depth);

                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_link(depth + 1, nth, link, &mut path_recycler))
                    .rev();

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                self.current.as_directory(
                    cid,
                    &name,
                    depth,
                    FileMetadata::from(&flat.data)
                );

                if !name.is_empty() {
                    path_recycler.push(name);
                }

                if let Some(next) = pending.pop() {
                    Ok(ContinuedWalk::Directory(Item {
                        state: State::Unfinished(Self {
                            current: self.current,
                            next: Some(next),
                            pending,
                            path_recycler,
                        })
                    }))
                } else {
                    Ok(ContinuedWalk::Directory(Item {
                        state: State::Last(self.current)
                    }))
                }
            },
            UnixFsType::HAMTShard => {
                let mut path_recycler = self.path_recycler;
                let (cid, name, depth) = self.next.expect("continued without next");

                //println!("continued over to HAMTShard {}, {:?}, {}", cid, name, depth);

                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(depth + 1, nth, link, &mut path_recycler))
                    .rev();

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                self.current.as_bucket(cid, &name, depth);

                if !name.is_empty() {
                    path_recycler.push(name);
                }

                if let Some(next) = pending.pop() {
                    Ok(ContinuedWalk::Directory(Item {
                        state: State::Unfinished(Self {
                            current: self.current,
                            next: Some(next),
                            pending,
                            path_recycler,
                        })
                    }))
                } else {
                    Ok(ContinuedWalk::Directory(Item {
                        state: State::Last(self.current)
                    }))
                }

            },
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, metadata, step) = IdleFileVisit::default()
                    .start_from_parsed(flat, cache)?;

                let (cid, name, depth) = self.next.expect("continued without next");
                //println!("continued over to File {}, {:?}, {}", cid, name, depth);
                let was_some = step.is_some();
                self.current.as_file(cid, &name, depth, metadata, step);
                self.path_recycler.push(name);

                let next = self.pending.pop();

                let segment = if was_some {
                    FileSegment::NotLast(bytes)
                } else {
                    FileSegment::Last(bytes)
                };

                if was_some || next.is_some() {

                    Ok(ContinuedWalk::File(segment, Item { state: State::Unfinished(Self {
                        current: self.current,
                        next,
                        pending: self.pending,
                        path_recycler: self.path_recycler,
                    })}))
                } else {
                    Ok(ContinuedWalk::File(segment, Item { state: State::Last(self.current) }))
                }
            },
            UnixFsType::Metadata => todo!("metadata?"),
            UnixFsType::Symlink => {
                let metadata = FileMetadata::from(&flat.data);
                let contents = flat.data.Data.as_deref().unwrap_or_default();

                let (cid, name, depth) = self.next.expect("continued without next");

                self.current.as_symlink(cid, &name, depth, metadata);

                if let Some(next) = self.pending.pop() {
                    Ok(ContinuedWalk::Symlink(bytes, Item { state: State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending: self.pending,
                        path_recycler: self.path_recycler,
                    })}))
                } else {
                    Ok(ContinuedWalk::Symlink(bytes, Item { state: State::Last(self.current) }))
                }
            },
        }
    }

    fn skip_current(&mut self) {
        use InnerKind::*;
        match &mut self.current.kind {
            File(_, visit @ Some(_)) => {
                visit.take();
            }
            _ => {},
        }
    }
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
    File(&'a Cid, &'a Path, &'a FileMetadata),
    Symlink(&'a Cid, &'a Path, &'a FileMetadata),
}

impl Entry<'_> {
    pub fn path(&self) -> &Path {
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
        //println!("as_dir -> {:?}", self.path);
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
                    // continuation bucket
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
        //println!("as_bucket -> {:?}", self.path);
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
        //println!("as_file -> {:?}", self.path);
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
    /// Walk was started on a file which can be iterated further with the given FileVisit
    MultiBlockFile {
        metadata: FileMetadata,
        visit: FileVisit,
    },
    Symlink {
        metadata: FileMetadata,
        target: &'a [u8],
    },
    /// Walk was started on a directory
    Walker(Walker),
}

#[derive(Debug)]
pub struct Item {
    state: State,
}

impl Item {
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
    // should this be just the description of what we just walked into?
    File(FileSegment<'a>, Item),
    Directory(Item),
    Symlink(&'a [u8], Item),
}

#[derive(Debug)]
pub enum FileSegment<'a> {
    NotLast(&'a [u8]),
    Last(&'a [u8]),
}

impl<'a> AsRef<[u8]> for FileSegment<'a> {
    fn as_ref(&self) -> &[u8] {
        use FileSegment::*;
        match self {
            NotLast(a) => a,
            Last(b) => b,
        }
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
