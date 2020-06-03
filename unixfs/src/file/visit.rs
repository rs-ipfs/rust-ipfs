use cid::Cid;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::ops::Range;

use crate::file::reader::{FileContent, FileReader, Traversal};
use crate::file::{FileMetadata, FileReadFailed, UnwrapBorrowedExt};
use crate::pb::merkledag::PBLink;

/// IdleFileVisit represents a prepared file visit over a tree. The user has to know the CID and be
/// able to get the block for the visit.
#[derive(Default, Debug)]
pub struct IdleFileVisit {
    range: Option<Range<u64>>,
}

impl IdleFileVisit {
    /// Target range represents the target byte range of the file we are interested in visiting.
    pub fn with_target_range(self, range: Range<u64>) -> Self {
        Self {
            range: Some(range),
        }
    }

    /// Begins the visitation by offering the first block to be visited.
    pub fn start<'a>(
        self,
        block: &'a [u8],
    ) -> Result<(&'a [u8], FileMetadata, Option<FileVisit>), FileReadFailed> {
        let fr = FileReader::from_block(block)?;

        let metadata = fr.as_ref().to_owned();

        let (content, traversal) = fr.content();

        match content {
            FileContent::Just(content) => {
                let block = 0..content.len() as u64;
                let content = maybe_target_slice(content, &block, self.range.as_ref());
                Ok((content, metadata, None))
            }
            FileContent::Spread(iter) => {
                // we need to select suitable here
                let mut pending = iter
                    .enumerate()
                    .filter_map(|(i, (link, range))| {
                        if !block_is_in_target_range(&range, self.range.as_ref()) {
                            return None;
                        }

                        Some(to_pending(i, link, range))
                    })
                    .collect::<Result<Vec<(Cid, Range<u64>)>, _>>()?;

                // order is reversed to consume them in the depth first order
                pending.reverse();

                if pending.is_empty() {
                    Ok((&[][..], metadata, None))
                } else {
                    Ok((
                        &[][..],
                        metadata,
                        Some(FileVisit {
                            pending,
                            state: traversal,
                            range: self.range,
                        }),
                    ))
                }
            }
        }
    }
}

fn to_pending(
    nth: usize,
    link: PBLink<'_>,
    range: Range<u64>,
) -> Result<(Cid, Range<u64>), FileReadFailed> {
    let hash = link.Hash.unwrap_borrowed();

    match Cid::try_from(hash) {
        Ok(cid) => Ok((cid, range)),
        Err(e) => Err(FileReadFailed::LinkInvalidCid {
            nth,
            hash: hash.to_vec(),
            name: match link.Name {
                Some(Cow::Borrowed(x)) => Cow::Owned(String::from(x)),
                Some(Cow::Owned(x)) => Cow::Owned(x),
                None => Cow::Borrowed(""),
            },
            cause: e,
        }),
    }
}

/// FileVisit represents an ongoing visitation over an UnixFs File tree.
///
/// The file visitor does **not** implement size validation of merkledag links at the moment. This
/// could be implmented with generational storage and it would require an u64 per link.
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
    /// returned by this iterator is the one which needs to be processed next with `continue_walk`.
    // FIXME: this must change to Cid
    pub fn pending_links(&self) -> impl Iterator<Item = &Cid> {
        self.pending.iter().rev().map(|(link, _)| link)
    }

    /// Continues the walk with the data for the first `pending_link` key.
    ///
    /// Returns on success a tuple of bytes and new version of `FileVisit` to continue the visit,
    /// if there is something more to visit.
    pub fn continue_walk<'a>(
        mut self,
        next: &'a [u8],
    ) -> Result<(&'a [u8], Option<Self>), FileReadFailed> {
        let traversal = self.state;
        let (_, range) = self
            .pending
            .pop()
            .expect("User called continue_walk there must have been a next link");

        // interesting, validation doesn't trigger if the range is the same?
        // FIXME: get rid of clone
        let fr = traversal.continue_walk(next, &range)?;
        let (content, traversal) = fr.content();
        match content {
            FileContent::Just(content) => {
                let content = maybe_target_slice(content, &range, self.range.as_ref());

                if !self.pending.is_empty() {
                    self.state = traversal;
                    Ok((content, Some(self)))
                } else {
                    Ok((content, None))
                }
            }
            FileContent::Spread(iter) => {
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
}

impl AsRef<FileMetadata> for FileVisit {
    fn as_ref(&self) -> &FileMetadata {
        self.state.as_ref()
    }
}

/// Returns true if the blocks byte offsets are interesting for our target range, false otherwise.
/// If there is no target, all blocks are of interest.
fn block_is_in_target_range(block: &Range<u64>, target: Option<&Range<u64>>) -> bool {
    use std::cmp::{max, min};

    if let Some(target) = target {
        max(block.start, target.start) <= min(block.end, target.end)
    } else {
        true
    }
}

/// Whenever we propagate the content from the tree upwards, we need to make sure it's inside the
/// range we were originally interested in.
fn maybe_target_slice<'a>(content: &'a [u8], block: &Range<u64>, target: Option<&Range<u64>>) -> &'a [u8] {
    if let Some(target) = target {
        target_slice(content, block, target)
    } else {
        content
    }
}

fn target_slice<'a>(content: &'a [u8], block: &Range<u64>, target: &Range<u64>) -> &'a [u8] {
    use std::cmp::min;

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
    fn slice_for_target() {
        use std::ops::Range;

        // turns out these examples are not easy to determine at all
        // writing out the type here avoids &b""[..] inside the array.
        let cases: &[(&[u8], u64, Range<u64>, &[u8])] = &[
            // xxxx xxxx cont ent_
            // ^^^^ ^^^^
            (b"content_", 8, 0..8,   b""),

            // xxxx xxxx cont ent_
            // ^^^^ ^^^^ ^
            (b"content_", 8, 0..9,   b"c"),

            // xxxx xxxx cont ent_
            //  ^^^ ^^^^ ^^^^ ^^^^ ...
            (b"content_", 8, 1..20,  b"content_"),

            // xxxx xxxx cont ent_
            //         ^ ^^^^ ^^^^ ...
            (b"content_", 8, 7..20,  b"content_"),

            // xxxx xxxx cont ent_
            //           ^^^^ ^^^^ ...
            (b"content_", 8, 8..20,  b"content_"),

            // xxxx xxxx cont ent_
            //            ^^^ ^^^^ ...
            (b"content_", 8, 9..20,  b"ontent_"),

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
            assert_eq!(sliced, *expected, "slice {:?} of block {:?}", target_range, block_range);
        }
    }
}
