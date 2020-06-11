///! UnixFS file support.
///!
///! Most usable for walking UnixFS file trees provided by the `visit::IdleFileVisit` and
///! `visit::FileVisit` types.
use crate::pb::{ParsingFailed, UnixFs, UnixFsType};
use crate::{InvalidCidInLink, UnexpectedNodeType};
use std::borrow::Cow;
use std::fmt;

/// Low level UnixFS file descriptor reader support.
pub mod reader;

/// Higher level API for visiting the file tree.
pub mod visit;

/// Container for the unixfs metadata, which can be present at the root of the file trees.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct FileMetadata {
    mode: Option<u32>,
    mtime: Option<(i64, u32)>,
}

// TODO: add way to get std::fs::Permissions out of this, or maybe some libc like type?
impl FileMetadata {
    /// Returns the full file mode, if one has been specified.
    ///
    /// The full file mode is originally read through `st_mode` field of `stat` struct defined in
    /// `sys/stat.h` and it's defining OpenGroup standard. Lowest 3 bytes will correspond to read,
    /// write, and execute rights per user, group, and other and 4th byte determines sticky bits,
    /// set user id or set group id. Following two bytes correspond to the different file types, as
    /// defined by the same OpenGroup standard:
    /// https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/sys_stat.h.html
    pub fn mode(&self) -> Option<u32> {
        self.mode
    }

    /// Returns the raw timestamp of last modification time, if specified.
    ///
    /// The timestamp is `(seconds, nanos)` similar to `std::time::Duration` with the exception of
    /// allowing seconds to be negative. The seconds are calculated from `1970-01-01 00:00:00` or
    /// the common "unix epoch".
    pub fn mtime(&self) -> Option<(i64, u32)> {
        self.mtime
    }
}

impl<'a> From<&'a UnixFs<'_>> for FileMetadata {
    fn from(data: &'a UnixFs<'_>) -> Self {
        let mode = data.mode;
        let mtime = data
            .mtime
            .clone()
            .map(|ut| (ut.Seconds, ut.FractionalNanoseconds.unwrap_or(0)));

        FileMetadata { mode, mtime }
    }
}

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
            UnexpectedType(UnexpectedNodeType(t)) => write!(
                fmt,
                "unexpected type for UnixFs: {} or {:?}",
                t,
                UnixFsType::from(*t)
            ),
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
    NonRootDefinesMetadata(FileMetadata),
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
mod tests {
    use cid::Cid;
    use std::collections::HashMap;
    use std::convert::TryFrom;

    use super::{reader::*, visit::*, UnwrapBorrowedExt};
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
        assert!(matches!(res, Ok((b"content", _, None))), "{:?}", res);
    }

    #[test]
    fn visiting_too_large_range_of_singleblock_file() {
        let res = IdleFileVisit::default()
            .with_target_range(500_000..600_000)
            .start(CONTENT_FILE);

        assert!(matches!(res, Ok((b"", _, None))), "{:?}", res);
    }

    #[test]
    fn empty_file() {
        let block = &hex!("0a0408021800");
        let fr = FileReader::from_block(block).unwrap();
        let (content, _) = fr.content();
        assert!(matches!(content, FileContent::Bytes(b"")), "{:?}", content);
    }

    #[derive(Default)]
    struct FakeBlockstore {
        blocks: HashMap<Cid, Vec<u8>>,
    }

    impl FakeBlockstore {
        fn get_by_cid<'a>(&'a self, cid: &Cid) -> &'a [u8] {
            self.blocks.get(cid).unwrap()
        }

        fn get_by_raw<'a>(&'a self, key: &[u8]) -> &'a [u8] {
            self.get_by_cid(&Cid::try_from(key).unwrap())
        }

        fn get_by_str<'a>(&'a self, key: &str) -> &'a [u8] {
            self.get_by_cid(&Cid::try_from(key).unwrap())
        }

        fn insert_v0(&mut self, block: &[u8]) -> Cid {
            use sha2::Digest;
            let mut sha = sha2::Sha256::new();
            sha.input(block);
            let result = sha.result();

            let mh = multihash::wrap(multihash::Code::Sha2_256, &result[..]);
            let cid = Cid::new_v0(mh).unwrap();

            assert!(
                self.blocks.insert(cid.clone(), block.to_vec()).is_none(),
                "duplicate cid {}",
                cid
            );

            cid
        }

        fn with_fixtures() -> Self {
            let mut this = Self::default();
            let foobar_blocks: &[&[u8]] = &[
                // root for "foobar\n" from go-ipfs 0.5 add -s size-2
                //     root
                //      |
                //  ----+-----
                //  |  |  |  |
                // fo ob ar  \n
                // QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6
                &hex!("12280a221220fef9fe1804942b35e19e145a03f9c9d5ca9c997dda0a9416f3f515a52f1b3ce11200180a12280a221220dfb94b75acb208fd4873d84872af58bd65c731770a7d4c0deeb4088e87390bfe1200180a12280a221220054497ae4e89812c83276a48e3e679013a788b7c0eb02712df15095c02d6cd2c1200180a12280a221220cc332ceb37dea7d3d7c00d1393117638d3ed963575836c6d44a24951e444cf5d120018090a0c080218072002200220022001"),
                // first bytes: fo
                &hex!("0a0808021202666f1802"),
                // ob
                &hex!("0a08080212026f621802"),
                // ar
                &hex!("0a080802120261721802"),
                // \n
                &hex!("0a07080212010a1801"),

                // same "foobar\n" but with go-ipfs 0.5 add --trickle -s size-2
                &hex!("12280a2212200f20a024ce0152161bc23e7234573374dfc3999143deaebf9b07b9c67318f9bd1200180a12280a221220b424253c25b5a7345fc7945732e363a12a790341b7c2d758516bbad5bbaab4461200180a12280a221220b7ab6350c604a885be9bd72d833f026b1915d11abe7e8dda5d0bca689342b7411200180a12280a221220a8a826652c2a3e93a751456e71139df086a1fedfd3bd9f232ad52ea1d813720e120018090a0c080218072002200220022001"),
                // the blocks have type raw instead of file, for some unknown reason
                &hex!("0a0808001202666f1802"),
                &hex!("0a08080012026f621802"),
                &hex!("0a080800120261721802"),
                &hex!("0a07080012010a1801"),
            ];

            for block in foobar_blocks {
                this.insert_v0(block);
            }

            this
        }
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

        let (content, _, mut step) = visit.start(blocks.get_by_str(start)).unwrap();
        ret.extend(content);

        while let Some(visit) = step {
            let (first, _) = visit.pending_links();
            let block = blocks.get_by_cid(first);

            let (content, next_step) = visit.continue_walk(block).unwrap();
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
