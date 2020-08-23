use crate::pb::{FlatUnixFs, PBLink, RangeLinks, UnixFsType};
use core::convert::TryFrom;
use core::fmt;
use core::ops::Range;

use crate::file::{FileError, FileReadFailed, Metadata, UnwrapBorrowedExt};

/// Navigates the UnixFs files, which are either:
///  - single block files which have everything needed to all of the contents
///  - multi block files which have trees of trees until Raw leaf blocks
///
/// The trees can have different shapes but it doesn't really matter for our depth-first approach.
/// For seeking, the each sub-tree linking node will have blocksizes for the trees representing
/// which the original file offsets covered by the tree.
///
/// A file doesn't know it's name. It only has a name when part of a directory, and then the name
/// is on a PbLink::Name. With UnixFs the names are always UTF-8. The root CID is not interesting
/// either: we just need the root block.
pub struct FileReader<'a> {
    offset: u64,
    end: Ending,
    links: Vec<PBLink<'a>>,
    data: &'a [u8],
    blocksizes: Vec<u64>,
    metadata: Metadata,
    file_size: u64,
}

impl AsRef<Metadata> for FileReader<'_> {
    fn as_ref(&self) -> &Metadata {
        &self.metadata
    }
}

// TODO: this could be Range ... It just seemed there seems to be "two kinds" of endings but in
// reality these are closer to two kinds of ranges or spans.
#[derive(Debug)]
enum Ending {
    /// The block represented a subtree without actual content
    TreeCoverage(u64),
    /// The block repressented a leaf with actual content
    Chunk(u64),
}

impl Ending {
    /// Checks wheter or not the next range is good to be processed next.
    fn check_is_suitable_next(&self, offset: u64, next: &Range<u64>) -> Result<(), FileError> {
        match self {
            Ending::TreeCoverage(cover_end) if next.start <= offset && &next.end > cover_end => {
                // tree must be collapsing; we cant have root be some smaller *file* range than
                // the child
                Err(FileError::TreeExpandsOnLinks)
            }
            Ending::TreeCoverage(cover_end) if &next.start < cover_end && &next.end > cover_end => {
                // when moving to sibling at the same height or above, its coverage must start
                // from where we stopped
                //
                // This has been separated instead of making the TreeExpandsOnLinks more general as
                // this might be a reasonable way with unixfs to reuse lower trees but no such
                // example has been found at least.
                Err(FileError::TreeOverlapsBetweenLinks)
            }
            Ending::TreeCoverage(_) if next.start < offset => Err(FileError::EarlierLink),
            Ending::Chunk(chunk_end) if &next.start != chunk_end => {
                // when continuing on from leaf node to either tree at above or a chunk at
                // next, the next must continue where we stopped
                Err(FileError::TreeJumpsBetweenLinks)
            }
            _ => Ok(()),
        }
    }
}

impl<'a> FileReader<'a> {
    /// Method for starting the file traversal. `data` is the raw data from unixfs block.
    pub fn from_block(data: &'a [u8]) -> Result<Self, FileReadFailed> {
        let inner = FlatUnixFs::try_from(data)?;
        let metadata = Metadata::from(&inner.data);
        Self::from_parts(inner, 0, metadata)
    }

    pub(crate) fn from_parsed(inner: FlatUnixFs<'a>) -> Result<Self, FileReadFailed> {
        let metadata = Metadata::from(&inner.data);
        Self::from_parts(inner, 0, metadata)
    }

    /// Called by Traversal to continue traversing a file tree traversal.
    fn from_continued(
        traversal: Traversal,
        offset: u64,
        data: &'a [u8],
    ) -> Result<Self, FileReadFailed> {
        let inner = FlatUnixFs::try_from(data)?;

        if inner.data.mode.is_some() || inner.data.mtime.is_some() {
            let metadata = Metadata::from(&inner.data);
            return Err(FileError::NonRootDefinesMetadata(metadata).into());
        }

        Self::from_parts(inner, offset, traversal.metadata)
    }

    fn from_parts(
        inner: FlatUnixFs<'a>,
        offset: u64,
        metadata: Metadata,
    ) -> Result<Self, FileReadFailed> {
        let empty_or_no_content = inner
            .data
            .Data
            .as_ref()
            .map(|cow| cow.as_ref().is_empty())
            .unwrap_or(true);
        let is_zero_bytes = inner.data.filesize.unwrap_or(0) == 0;

        if inner.data.Type != UnixFsType::File && inner.data.Type != UnixFsType::Raw {
            Err(FileReadFailed::UnexpectedType(inner.data.Type.into()))
        } else if inner.links.len() != inner.data.blocksizes.len() {
            Err(FileError::LinksAndBlocksizesMismatch.into())
        } else if empty_or_no_content && !is_zero_bytes && inner.links.is_empty() {
            Err(FileError::NoLinksNoContent.into())
        } else {
            // raw and file seem to be same except the raw is preferred in trickle dag
            let data = inner.data.Data.unwrap_borrowed_or_empty();

            if inner.data.hashType.is_some() || inner.data.fanout.is_some() {
                return Err(FileError::UnexpectedRawOrFileProperties {
                    hash_type: inner.data.hashType,
                    fanout: inner.data.fanout,
                }
                .into());
            }

            let end = if inner.links.is_empty() {
                // can unwrap because `data` is all of the data
                let filesize = inner.data.filesize.unwrap_or(data.len() as u64);
                Ending::Chunk(offset + filesize)
            } else {
                match inner.data.filesize {
                    Some(filesize) => Ending::TreeCoverage(offset + filesize),
                    None => return Err(FileError::IntermediateNodeWithoutFileSize.into()),
                }
            };

            Ok(Self {
                offset,
                end,
                links: inner.links,
                data,
                blocksizes: inner.data.blocksizes,
                metadata,
                file_size: inner.data.filesize.unwrap(),
            })
        }
    }

    /// Returns a moved tuple of the content (bytes or links) and a traversal, which can be used to
    /// continue the traversal from the next block.
    pub fn content(
        self,
    ) -> (
        FileContent<'a, impl Iterator<Item = (PBLink<'a>, Range<u64>)>>,
        Traversal,
    ) {
        let traversal = Traversal {
            last_ending: self.end,
            last_offset: self.offset,

            metadata: self.metadata,
            file_size: self.file_size,
        };

        let fc = if self.links.is_empty() {
            FileContent::Bytes(self.data)
        } else {
            let zipped = self.links.into_iter().zip(self.blocksizes.into_iter());
            FileContent::Links(RangeLinks::from_links_and_blocksizes(
                zipped,
                Some(self.offset),
            ))
        };

        (fc, traversal)
    }
}

/// Carrier of validation data used between blocks during a walk on the merkle tree.
#[derive(Debug)]
pub struct Traversal {
    last_ending: Ending,
    last_offset: u64,
    file_size: u64,

    metadata: Metadata,
}

impl Traversal {
    /// Continues the walk on the merkle tree with the given block contents. The block contents is
    /// not validated and the range is expected to be the next from previous call to
    /// FileContent::Links iterator.
    ///
    /// When calling this directly, it is good to note that repeatedly calling this with the same
    /// block contents will not be detected, and will instead grow the internal Vec of links until
    /// memory runs out.
    pub fn continue_walk<'a>(
        self,
        next_block: &'a [u8],
        tree_range: &Range<u64>,
    ) -> Result<FileReader<'a>, FileReadFailed> {
        self.last_ending
            .check_is_suitable_next(self.last_offset, tree_range)?;
        FileReader::from_continued(self, tree_range.start, next_block)
    }

    /// Returns the total size of the file.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

impl AsRef<Metadata> for Traversal {
    fn as_ref(&self) -> &Metadata {
        &self.metadata
    }
}

/// Files in unixfs merkle trees can either contain content of the file, or can contain links to
/// other parts of the tree.
pub enum FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)> + 'a,
{
    /// When reaching the leaf level of a DAG we finally find the actual content. For empty files
    /// without content this will be an empty slice.
    Bytes(&'a [u8]),
    /// The content of the file is spread over a number of blocks; iteration must follow from index
    /// depth-first from the first link to reach the given the bytes in the given byte offset
    /// range.
    Links(I),
}

#[cfg(test)]
impl<'a, I> FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)>,
{
    /// Returns the content as bytes, or panics if there were links instead.
    pub fn unwrap_content(self) -> &'a [u8] {
        match self {
            FileContent::Bytes(x) => x,
            y => panic!("Expected FileContent::Bytes, found: {:?}", y),
        }
    }
}

impl<'a, I> fmt::Debug for FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileContent::Bytes(bytes) => write!(fmt, "Bytes({} bytes)", bytes.len()),
            FileContent::Links(iter) => write!(fmt, "Links({:?})", iter.size_hint()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ending;
    use crate::file::FileError;

    #[test]
    fn collapsing_tree() {
        // this is pretty much how I planned the ending might be useful but it's perhaps a bit
        // confusing as it's only the half of the range
        Ending::TreeCoverage(100)
            .check_is_suitable_next(0, &(0..100))
            .unwrap();
        Ending::TreeCoverage(100)
            .check_is_suitable_next(0, &(0..10))
            .unwrap();
        Ending::TreeCoverage(100)
            .check_is_suitable_next(0, &(0..2))
            .unwrap();
        Ending::Chunk(2)
            .check_is_suitable_next(0, &(2..10))
            .unwrap();
        Ending::TreeCoverage(10)
            .check_is_suitable_next(2, &(2..10))
            .unwrap();
        Ending::TreeCoverage(10)
            .check_is_suitable_next(2, &(10..20))
            .unwrap();
        Ending::Chunk(10)
            .check_is_suitable_next(2, &(10..100))
            .unwrap();
    }

    #[test]
    fn expanding_tree() {
        let res = Ending::TreeCoverage(100).check_is_suitable_next(10, &(0..102));
        assert!(
            matches!(res, Err(FileError::TreeExpandsOnLinks)),
            "{:?}",
            res
        );

        let res = Ending::TreeCoverage(100).check_is_suitable_next(0, &(0..102));
        assert!(
            matches!(res, Err(FileError::TreeExpandsOnLinks)),
            "{:?}",
            res
        );
    }

    #[test]
    fn overlap() {
        let res = Ending::TreeCoverage(100).check_is_suitable_next(10, &(88..102));
        assert!(
            matches!(res, Err(FileError::TreeOverlapsBetweenLinks)),
            "{:?}",
            res
        );
    }

    #[test]
    fn hole() {
        let res = Ending::Chunk(100).check_is_suitable_next(0, &(101..105));
        assert!(
            matches!(res, Err(FileError::TreeJumpsBetweenLinks)),
            "{:?}",
            res
        );
    }

    #[test]
    fn wrong_next() {
        let res = Ending::TreeCoverage(200).check_is_suitable_next(100, &(0..100));
        assert!(matches!(res, Err(FileError::EarlierLink)), "{:?}", res);

        let res = Ending::TreeCoverage(101).check_is_suitable_next(100, &(0..100));
        assert!(matches!(res, Err(FileError::EarlierLink)), "{:?}", res);

        let res = Ending::TreeCoverage(100).check_is_suitable_next(100, &(0..100));
        assert!(matches!(res, Err(FileError::EarlierLink)), "{:?}", res);
    }
}
