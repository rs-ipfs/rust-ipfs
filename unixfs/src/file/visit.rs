use cid::Cid;
use core::convert::TryFrom;
use core::ops::Range;

use crate::file::reader::{FileContent, FileReader, Traversal};
use crate::file::{FileReadFailed, Metadata};
use crate::pb::{merkledag::PBLink, FlatUnixFs};
use crate::InvalidCidInLink;

/// IdleFileVisit represents a prepared file visit over a tree. The user has to know the CID and be
/// able to get the block for the visit.
///
/// **Note**: For easier to use interface, you should consider using `ipfs_unixfs::walk::Walker`.
/// It uses `IdleFileVisit` and `FileVisit` internally but has a better API.
#[derive(Default, Debug)]
pub struct IdleFileVisit {
    range: Option<Range<u64>>,
}

type FileVisitResult<'a> = (&'a [u8], u64, Metadata, Option<FileVisit>);

impl IdleFileVisit {
    /// Target range represents the target byte range of the file we are interested in visiting.
    pub fn with_target_range(self, range: Range<u64>) -> Self {
        Self { range: Some(range) }
    }

    /// Begins the visitation by processing the first block to be visited.
    ///
    /// Returns (on success) a tuple of file bytes, total file size, any metadata associated, and
    /// optionally a `FileVisit` to continue the walk.
    pub fn start(self, block: &'_ [u8]) -> Result<FileVisitResult<'_>, FileReadFailed> {
        let fr = FileReader::from_block(block)?;
        self.start_from_reader(fr, &mut None)
    }

    pub(crate) fn start_from_parsed<'a>(
        self,
        block: FlatUnixFs<'a>,
        cache: &'_ mut Option<Cache>,
    ) -> Result<FileVisitResult<'a>, FileReadFailed> {
        let fr = FileReader::from_parsed(block)?;
        self.start_from_reader(fr, cache)
    }

    fn start_from_reader<'a>(
        self,
        fr: FileReader<'a>,
        cache: &'_ mut Option<Cache>,
    ) -> Result<FileVisitResult<'a>, FileReadFailed> {
        let metadata = fr.as_ref().to_owned();

        let (content, traversal) = fr.content();

        match content {
            FileContent::Bytes(content) => {
                let block = 0..content.len() as u64;
                let content = maybe_target_slice(content, &block, self.range.as_ref());
                Ok((content, traversal.file_size(), metadata, None))
            }
            FileContent::Links(iter) => {
                // we need to select suitable here
                let mut links = cache.take().unwrap_or_default().inner;

                let pending = iter.enumerate().filter_map(|(i, (link, range))| {
                    if !block_is_in_target_range(&range, self.range.as_ref()) {
                        return None;
                    }

                    Some(to_pending(i, link, range))
                });

                for item in pending {
                    links.push(item?);
                }

                // order is reversed to consume them in the depth first order
                links.reverse();

                if links.is_empty() {
                    *cache = Some(links.into());
                    Ok((&[][..], traversal.file_size(), metadata, None))
                } else {
                    Ok((
                        &[][..],
                        traversal.file_size(),
                        metadata,
                        Some(FileVisit {
                            pending: links,
                            state: traversal,
                            range: self.range,
                        }),
                    ))
                }
            }
        }
    }
}

/// Optional cache for datastructures which can be re-used without re-allocation between walks of
/// different files.
#[derive(Default)]
pub struct Cache {
    inner: Vec<(Cid, Range<u64>)>,
}

impl From<Vec<(Cid, Range<u64>)>> for Cache {
    fn from(mut inner: Vec<(Cid, Range<u64>)>) -> Self {
        inner.clear();
        Cache { inner }
    }
}

/// FileVisit represents an ongoing visitation over an UnixFs File tree.
///
/// The file visitor does **not** implement size validation of merkledag links at the moment. This
/// could be implmented with generational storage and it would require an u64 per link.
///
/// **Note**: For easier to use interface, you should consider using `ipfs_unixfs::walk::Walker`.
/// It uses `IdleFileVisit` and `FileVisit` internally but has a better API.
#[derive(Debug)]
pub struct FileVisit {
    /// The internal cache for pending work. Order is such that the next is always the last item,
    /// so it can be popped. This currently does use a lot of memory for very large files.
    ///
    /// One workaround would be to transform excess links to relative links to some block of a Cid.
    pending: Vec<(Cid, Range<u64>)>,
    /// Target range, if any. Used to filter the links so that we will only visit interesting
    /// parts.
    range: Option<Range<u64>>,
    state: Traversal,
}

impl FileVisit {
    /// Access hashes of all pending links for prefetching purposes. The block for the first item
    /// returned by this method is the one which needs to be processed next with `continue_walk`.
    ///
    /// Returns tuple of the next Cid which needs to be processed and an iterator over the
    /// remaining.
    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid>) {
        let mut iter = self.pending.iter().rev().map(|(link, _)| link);
        let first = iter
            .next()
            .expect("the presence of links has been validated");
        (first, iter)
    }

    /// Continues the walk with the data for the first `pending_link` key.
    ///
    /// Returns on success a tuple of bytes and new version of `FileVisit` to continue the visit,
    /// when there is something more to visit.
    pub fn continue_walk<'a>(
        mut self,
        next: &'a [u8],
        cache: &mut Option<Cache>,
    ) -> Result<(&'a [u8], Option<Self>), FileReadFailed> {
        let traversal = self.state;
        let (_, range) = self
            .pending
            .pop()
            .expect("User called continue_walk there must have been a next link");

        // interesting, validation doesn't trigger if the range is the same?
        let fr = traversal.continue_walk(next, &range)?;
        let (content, traversal) = fr.content();
        match content {
            FileContent::Bytes(content) => {
                let content = maybe_target_slice(content, &range, self.range.as_ref());

                if !self.pending.is_empty() {
                    self.state = traversal;
                    Ok((content, Some(self)))
                } else {
                    *cache = Some(self.pending.into());
                    Ok((content, None))
                }
            }
            FileContent::Links(iter) => {
                let before = self.pending.len();

                for (i, (link, range)) in iter.enumerate() {
                    if !block_is_in_target_range(&range, self.range.as_ref()) {
                        continue;
                    }

                    self.pending.push(to_pending(i, link, range)?);
                }

                // reverse to keep the next link we need to traverse as last, where pop() operates.
                (&mut self.pending[before..]).reverse();

                self.state = traversal;
                Ok((&[][..], Some(self)))
            }
        }
    }

    /// Returns the total size of the file in bytes.
    pub fn file_size(&self) -> u64 {
        self.state.file_size()
    }
}

impl AsRef<Metadata> for FileVisit {
    fn as_ref(&self) -> &Metadata {
        self.state.as_ref()
    }
}

fn to_pending(
    nth: usize,
    link: PBLink<'_>,
    range: Range<u64>,
) -> Result<(Cid, Range<u64>), FileReadFailed> {
    let hash = link.Hash.as_deref().unwrap_or_default();

    match Cid::try_from(hash) {
        Ok(cid) => Ok((cid, range)),
        Err(e) => Err(FileReadFailed::InvalidCid(InvalidCidInLink::from((
            nth, link, e,
        )))),
    }
}

/// Returns true if the blocks byte offsets are interesting for our target range, false otherwise.
/// If there is no target, all blocks are of interest.
fn block_is_in_target_range(block: &Range<u64>, target: Option<&Range<u64>>) -> bool {
    use core::cmp::{max, min};

    if let Some(target) = target {
        max(block.start, target.start) <= min(block.end, target.end)
    } else {
        true
    }
}

/// Whenever we propagate the content from the tree upwards, we need to make sure it's inside the
/// range we were originally interested in.
fn maybe_target_slice<'a>(
    content: &'a [u8],
    block: &Range<u64>,
    target: Option<&Range<u64>>,
) -> &'a [u8] {
    if let Some(target) = target {
        target_slice(content, block, target)
    } else {
        content
    }
}

fn target_slice<'a>(content: &'a [u8], block: &Range<u64>, target: &Range<u64>) -> &'a [u8] {
    use core::cmp::min;

    if !block_is_in_target_range(block, Some(target)) {
        // defaulting to empty slice is good, and similar to the "cat" HTTP API operation.
        &[][..]
    } else {
        let start;
        let end;

        // FIXME: these must have bugs and must be possible to simplify
        if target.start < block.start {
            // we mostly need something before
            start = 0;
            end = (min(target.end, block.end) - block.start) as usize;
        } else if target.end > block.end {
            // we mostly need something after
            start = (target.start - block.start) as usize;
            end = (min(target.end, block.end) - block.start) as usize;
        } else {
            // inside
            start = (target.start - block.start) as usize;
            end = start + (target.end - target.start) as usize;
        }

        &content[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::target_slice;

    #[test]
    #[allow(clippy::type_complexity)]
    fn slice_for_target() {
        use core::ops::Range;

        // turns out these examples are not easy to determine at all
        // writing out the type here avoids &b""[..] inside the array.
        let cases: &[(&[u8], u64, Range<u64>, &[u8])] = &[
            // xxxx xxxx cont ent_
            // ^^^^ ^^^^
            (b"content_", 8, 0..8, b""),
            // xxxx xxxx cont ent_
            // ^^^^ ^^^^ ^
            (b"content_", 8, 0..9, b"c"),
            // xxxx xxxx cont ent_
            //  ^^^ ^^^^ ^^^^ ^^^^ ...
            (b"content_", 8, 1..20, b"content_"),
            // xxxx xxxx cont ent_
            //         ^ ^^^^ ^^^^ ...
            (b"content_", 8, 7..20, b"content_"),
            // xxxx xxxx cont ent_
            //           ^^^^ ^^^^ ...
            (b"content_", 8, 8..20, b"content_"),
            // xxxx xxxx cont ent_
            //            ^^^ ^^^^ ...
            (b"content_", 8, 9..20, b"ontent_"),
            // xxxx xxxx cont ent_
            //                   ^ ...
            (b"content_", 8, 15..20, b"_"),
            // xxxx xxxx cont ent_ yyyy
            //                     ^^^^
            (b"content_", 8, 16..20, b""),
        ];

        for (block_data, block_offset, target_range, expected) in cases {
            let block_range = *block_offset..(block_offset + block_data.len() as u64);
            let sliced = target_slice(block_data, &block_range, target_range);
            assert_eq!(
                sliced, *expected,
                "slice {:?} of block {:?}",
                target_range, block_range
            );
        }
    }
}
