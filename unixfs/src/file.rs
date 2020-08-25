///! UnixFS file support.
///!
///! The module provides low-level File tree visitor support and file importing support. Note: The
///! [`ipfs_unixfs::walk::Walker`] should typically be used for accessing file content.
use crate::pb::ParsingFailed;
use crate::{InvalidCidInLink, Metadata, UnexpectedNodeType};
use alloc::borrow::Cow;
use core::fmt;

/// Low level UnixFS file descriptor reader support.
mod reader;

/// Mid level API for visiting the file tree.
pub mod visit;

/// File adder capable of constructing UnixFs v1 trees
pub mod adder;

/// Describes the errors which can happen during a visit or lower level block-by-block walking of
/// the DAG.
#[derive(Debug)]
pub enum FileReadFailed {
    /// Unsupported UnixFs file; these might exist, but currently there are no workarounds for
    /// handling them.
    File(FileError),
    /// FileReader can only process raw or file type of unixfs content.
    // This is the raw value instead of the enum by design not to expose the quick-protobuf types
    UnexpectedType(UnexpectedNodeType),
    /// Parsing failed
    Read(Option<quick_protobuf::Error>),
    /// Link could not be turned into Cid.
    InvalidCid(InvalidCidInLink),
}

impl fmt::Display for FileReadFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FileReadFailed::*;

        match self {
            File(e) => write!(fmt, "{}", e),
            UnexpectedType(ut) => write!(fmt, "unexpected type for UnixFs: {:?}", ut),
            Read(Some(e)) => write!(fmt, "reading failed: {}", e),
            Read(None) => write!(fmt, "reading failed: missing UnixFS message"),
            InvalidCid(e) => write!(fmt, "{}", e),
        }
    }
}

impl std::error::Error for FileReadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use FileReadFailed::*;
        match self {
            InvalidCid(e) => Some(e),
            Read(Some(e)) => Some(e),
            _ => None,
        }
    }
}

impl<'a> From<ParsingFailed<'a>> for FileReadFailed {
    fn from(e: ParsingFailed<'a>) -> Self {
        use ParsingFailed::*;
        match e {
            InvalidDagPb(e) => FileReadFailed::Read(Some(e)),
            InvalidUnixFs(e, _) => FileReadFailed::Read(Some(e)),
            NoData(_) => FileReadFailed::Read(None),
        }
    }
}

/// Errors which can happen while processing UnixFS type File or Raw blocks.
#[derive(Debug, PartialEq, Eq)]
pub enum FileError {
    /// There are nonequal number of links and blocksizes and thus the file ranges for linked trees
    /// or blocks cannot be determined.
    LinksAndBlocksizesMismatch,
    /// Errored when the filesize is non-zero.
    NoLinksNoContent,
    /// Unsupported: non-root block defines metadata.
    NonRootDefinesMetadata(Metadata),
    /// A non-leaf node in the tree has no filesize value which is used to determine the file range
    /// for this tree.
    IntermediateNodeWithoutFileSize,
    /// The tree or merkle dag should only collapse or stay the same length.
    TreeExpandsOnLinks,
    /// The tree links contain overlapping file segments. This is at least unsupported right now,
    /// but the larger segment could be collapsed down to the reused part.
    TreeOverlapsBetweenLinks,
    /// Reader has been fed a link to earlier range.
    EarlierLink,
    /// The tree links contain a hole from a file segment to the next tree. This is at least
    /// unsupported right now. Zeroes could be generated for the hole.
    TreeJumpsBetweenLinks,
    /// These values should not be present for unixfs files with File or Raw. If they have a valid
    /// meaning, support for such has not been implemented.
    UnexpectedRawOrFileProperties {
        /// Hash type, as read from the protobuf descriptor; should only be used with HAMT
        /// directories.
        hash_type: Option<u64>,
        /// Fan out, as read from the protobuf descriptor; should only be used with HAMT
        /// directories.
        fanout: Option<u64>,
    },
}

impl fmt::Display for FileError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FileError::*;
        match self {
            LinksAndBlocksizesMismatch => write!(
                fmt,
                "different number of links and blocksizes: cannot determine subtree ranges"
            ),
            NoLinksNoContent => write!(
                fmt,
                "filesize is non-zero while there are no links or content"
            ),
            NonRootDefinesMetadata(metadata) => {
                write!(fmt, "unsupported: non-root defines {:?}", metadata)
            }
            IntermediateNodeWithoutFileSize => {
                write!(fmt, "intermediatery node with links but no filesize")
            }
            TreeExpandsOnLinks => write!(
                fmt,
                "total size of tree expands through links, it should only get smaller or keep size"
            ),
            TreeOverlapsBetweenLinks => write!(fmt, "unsupported: tree contains overlap"),
            EarlierLink => write!(fmt, "error: earlier link given"),
            TreeJumpsBetweenLinks => write!(fmt, "unsupported: tree contains holes"),
            UnexpectedRawOrFileProperties { hash_type, fanout } => write!(
                fmt,
                "unsupported: File or Raw with hash_type {:?} or fanount {:?}",
                hash_type, fanout
            ),
        }
    }
}

impl std::error::Error for FileError {}

impl From<FileError> for FileReadFailed {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

/// This exists to help matching the borrowed slice in `Option<Cow<'_, [u8]>>` in this file
/// or defaulting to empty array. In the processing inside this file, the Cow never represents
/// owned value.
///
/// This at least sounded useful early on as the quick-protobuf produces many Option<Cow> values
/// which are a bit tricky to handle. We never turn them into Option<Cow::Owned> so we can safely
/// use these.
pub(crate) trait UnwrapBorrowedExt<'a> {
    /// Does not default but requires there to be an borrowed inner value.
    fn unwrap_borrowed(self) -> &'a [u8];

    /// Unwraps the Cow of [u8] into empty or wrapped slice.
    fn unwrap_borrowed_or_empty(self) -> &'a [u8]
    where
        Self: 'a;
}

impl<'a> UnwrapBorrowedExt<'a> for Option<Cow<'a, [u8]>> {
    fn unwrap_borrowed(self) -> &'a [u8] {
        match self {
            Some(Cow::Borrowed(x)) => x,
            Some(Cow::Owned(_)) => panic!("unexpected Cow::Owned"),
            None => panic!("Unexpected None"),
        }
    }
    fn unwrap_borrowed_or_empty(self) -> &'a [u8] {
        match self {
            Some(Cow::Borrowed(x)) => x,
            None => &[][..],
            _ => panic!("should not be Cow::Owned"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{reader::*, visit::*, UnwrapBorrowedExt};
    use crate::test_support::FakeBlockstore;
    use hex_literal::hex;

    const CONTENT_FILE: &[u8] = &hex!("0a0d08021207636f6e74656e741807");

    #[test]
    fn just_content() {
        let fr = FileReader::from_block(CONTENT_FILE).unwrap();
        let (content, _) = fr.content();
        assert!(
            matches!(content, FileContent::Bytes(b"content")),
            "{:?}",
            content
        );
    }

    #[test]
    fn visiting_just_content() {
        let res = IdleFileVisit::default().start(CONTENT_FILE);
        assert!(matches!(res, Ok((b"content", _, _, None))), "{:?}", res);
    }

    #[test]
    fn visiting_too_large_range_of_singleblock_file() {
        let res = IdleFileVisit::default()
            .with_target_range(500_000..600_000)
            .start(CONTENT_FILE);

        assert!(matches!(res, Ok((b"", _, _, None))), "{:?}", res);
    }

    #[test]
    fn empty_file() {
        let block = &hex!("0a0408021800");
        let fr = FileReader::from_block(block).unwrap();
        let (content, _) = fr.content();
        assert!(matches!(content, FileContent::Bytes(b"")), "{:?}", content);
    }

    #[test]
    fn balanced_traversal() {
        let target = "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6";
        let blocks = FakeBlockstore::with_fixtures();

        // filled on root
        let (mut links_and_ranges, mut traversal) = {
            let root = FileReader::from_block(blocks.get_by_str(target)).unwrap();

            let (mut links_and_ranges, traversal) = match root.content() {
                (FileContent::Links(iter), traversal) => {
                    let links_and_ranges = iter
                        .map(|(link, range)| (link.Hash.unwrap_borrowed().to_vec(), range))
                        .collect::<Vec<_>>();
                    (links_and_ranges, traversal)
                }
                x => unreachable!("unexpected {:?}", x),
            };

            // reverse again to pop again
            links_and_ranges.reverse();
            // something 'static to hold on between two blocks
            (links_and_ranges, traversal)
        };

        let mut combined: Vec<u8> = Vec::new();

        while let Some((key, range)) = links_and_ranges.pop() {
            let next = blocks.get_by_raw(&key);
            let fr = traversal.continue_walk(&next, &range).unwrap();

            let (content, next) = fr.content();
            combined.extend(content.unwrap_content());
            traversal = next;
        }

        assert_eq!(combined, b"foobar\n");
    }

    fn collect_bytes(blocks: &FakeBlockstore, visit: IdleFileVisit, start: &str) -> Vec<u8> {
        let mut ret = Vec::new();

        let (content, _, _, mut step) = visit.start(blocks.get_by_str(start)).unwrap();
        ret.extend(content);

        while let Some(visit) = step {
            let (first, _) = visit.pending_links();
            let block = blocks.get_by_cid(first);

            let (content, next_step) = visit.continue_walk(block, &mut None).unwrap();
            ret.extend(content);
            step = next_step;
        }

        ret
    }

    #[test]
    fn visitor_traversal() {
        let blocks = FakeBlockstore::with_fixtures();

        let start = "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6";
        let bytes = collect_bytes(&blocks, IdleFileVisit::default(), start);

        assert_eq!(&bytes[..], b"foobar\n");
    }

    #[test]
    fn scoped_visitor_traversal_from_blockstore() {
        let blocks = FakeBlockstore::with_fixtures();

        let start = "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6";
        let visit = IdleFileVisit::default().with_target_range(1..6);
        let bytes = collect_bytes(&blocks, visit, start);

        assert_eq!(&bytes[..], b"oobar");
    }

    #[test]
    fn less_than_block_scoped_traversal_from_blockstore() {
        let blocks = FakeBlockstore::with_fixtures();

        let start = "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6";
        let visit = IdleFileVisit::default().with_target_range(0..1);
        let bytes = collect_bytes(&blocks, visit, start);

        assert_eq!(&bytes[..], b"f");
    }

    #[test]
    fn scoped_traversal_out_of_bounds_from_blockstore() {
        let blocks = FakeBlockstore::with_fixtures();

        let start = "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6";
        let visit = IdleFileVisit::default().with_target_range(7..20);
        let bytes = collect_bytes(&blocks, visit, start);

        assert_eq!(&bytes[..], b"");
    }

    #[test]
    fn trickle_traversal() {
        let blocks = FakeBlockstore::with_fixtures();

        let start = "QmWfQ48ChJUj4vWKFsUDe4646xCBmXgdmNfhjz9T7crywd";
        let bytes = collect_bytes(&blocks, IdleFileVisit::default(), start);

        assert_eq!(&bytes[..], b"foobar\n");
    }
}
