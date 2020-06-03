use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Range;

pub(crate) mod merkledag;
pub(crate) use merkledag::PBLink;
pub(crate) use merkledag::PBNode;

pub(crate) mod unixfs;
pub(crate) use unixfs::mod_Data::DataType as UnixFsType;
pub(crate) use unixfs::Data as UnixFs;

/// Failure cases for TryFrom conversions and most importantly, the `FlatUnixFs` conversion.
#[derive(Debug)]
pub enum UnixFsReadFailed {
    /// The outer content could not be read as dag-pb PBNode.
    InvalidDagPb(quick_protobuf::Error),
    /// dag-pb::PBNode::Data could not be read as UnixFS::Data message
    InvalidUnixFs(quick_protobuf::Error),
    /// dag-pb contained zero bytes
    NoData,
}

impl fmt::Display for UnixFsReadFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UnixFsReadFailed::*;
        match self {
            InvalidDagPb(e) => write!(fmt, "failed to read the block as dag-pb: {}", e),
            InvalidUnixFs(e) => write!(
                fmt,
                "failed to read the dag-pb PBNode::Data as UnixFS message: {}",
                e
            ),
            NoData => write!(fmt, "dag-pb PBNode::Data was missing or empty"),
        }
    }
}

impl std::error::Error for UnixFsReadFailed {}

impl<'a> TryFrom<&'a merkledag::PBNode<'a>> for unixfs::Data<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(node: &'a merkledag::PBNode<'a>) -> Result<Self, Self::Error> {
        UnixFs::try_from(node.Data.as_ref().map(|a| a.as_ref()))
    }
}

impl<'a> TryFrom<Option<&'a [u8]>> for unixfs::Data<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(data: Option<&'a [u8]>) -> Result<Self, Self::Error> {
        use quick_protobuf::{BytesReader, MessageRead};

        let data: &'a [u8] = data.ok_or(UnixFsReadFailed::NoData)?;
        let mut reader = BytesReader::from_bytes(data);
        UnixFs::from_reader(&mut reader, data).map_err(UnixFsReadFailed::InvalidUnixFs)
    }
}

// These should be derived by the pb-rs
impl<'a> TryFrom<&'a [u8]> for merkledag::PBNode<'a> {
    type Error = quick_protobuf::Error;

    fn try_from(data: &'a [u8]) -> Result<Self, Self::Error> {
        use quick_protobuf::{BytesReader, MessageRead};
        merkledag::PBNode::from_reader(&mut BytesReader::from_bytes(data), data)
    }
}

/// Intermediate conversion structure suitable for creating multiple kinds of readers.
pub(crate) struct FlatUnixFs<'a> {
    pub(crate) links: Vec<PBLink<'a>>,
    pub(crate) data: UnixFs<'a>,
}

impl<'a> TryFrom<&'a [u8]> for FlatUnixFs<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(data: &'a [u8]) -> Result<Self, Self::Error> {
        let PBNode {
            Links: links,
            Data: data,
        } = merkledag::PBNode::try_from(data).map_err(UnixFsReadFailed::InvalidDagPb)?;

        let inner = UnixFs::try_from(match data {
            Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => Some(bytes),
            Some(Cow::Owned(_)) => unreachable!(),
            Some(Cow::Borrowed(_)) | None => return Err(UnixFsReadFailed::NoData),
        })?;

        Ok(FlatUnixFs { links, data: inner })
    }
}

#[cfg(test)]
impl<'a> FlatUnixFs<'a> {
    pub fn range_links(&'a self) -> impl Iterator<Item = (PBLink<'a>, Range<u64>)> {
        assert_eq!(self.links.len(), self.data.blocksizes.len());

        let zipped = self
            .links
            .clone()
            .into_iter()
            .zip(self.data.blocksizes.iter().copied());

        // important: we have validated links.len() == blocksizes.len()
        RangeLinks::from_links_and_blocksizes(zipped, Some(0))
    }
}

pub(crate) struct RangeLinks<I> {
    inner: I,
    base: u64,
}

impl<'a, I> RangeLinks<I>
where
    I: Iterator<Item = (PBLink<'a>, u64)>,
{
    /// `start_offset` is the offset of the current tree when walking the graph.
    pub fn from_links_and_blocksizes(zipped: I, start_offset: Option<u64>) -> RangeLinks<I> {
        RangeLinks {
            inner: zipped,
            base: start_offset.unwrap_or(0),
        }
    }
}

impl<'a, I> Iterator for RangeLinks<I>
where
    I: Iterator<Item = (PBLink<'a>, u64)>,
{
    type Item = (PBLink<'a>, Range<u64>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((link, blocksize)) => {
                let returned_base = self.base;
                self.base += blocksize;
                Some((link, returned_base..(returned_base + blocksize)))
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod test {

    use super::{FlatUnixFs, PBNode, UnixFs, UnixFsType};
    use std::borrow::Cow;
    use std::convert::TryFrom;
    use std::fmt;

    #[test]
    fn hello_world() {
        use quick_protobuf::{BytesReader, MessageRead};
        let input = &[
            0x0a, 0x0d, 0x08, 0x02, 0x12, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x18,
            0x07,
        ];

        // this does not work, because this reads a length delimited message
        //let dagnode: merkledag::PBNode =
        //    deserialize_from_slice(input).expect("parse outer dagpb node");

        let mut reader = BytesReader::from_bytes(input);
        let dagnode =
            PBNode::from_reader(&mut reader, input).expect("parse outer merkledag::PBNode");
        assert!(dagnode.Links.is_empty());
        println!("{:?}", dagnode);

        let unixfs_data = UnixFs::try_from(&dagnode).expect("parse inner unixfs::Data");
        assert_eq!(unixfs_data.Type, UnixFsType::File);
        assert_eq!(unixfs_data.Data, Some(Cow::Borrowed(&b"content"[..])));
        println!("{:?}", unixfs_data);
    }

    #[test]
    fn linux_tarxz_range_links() {
        // Cannot trace how this top level block was in fact created
        let input = &[
            // 0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x48, 0xb6, 0xfa, 0x7a, 0x97, 0xa1, 0x06, 0x15,
            // 0x3f, 0xaa, 0xc3, 0xab, 0x3c, 0x5a, 0x02, 0x01, 0x00, 0x8e, 0xab, 0x98, 0x1d, 0x58,
            // 0x0b, 0x95, 0x18, 0xcb, 0xa7, 0xf0, 0x71, 0x49, 0x30, 0x1d, 0x12, 0x00, 0x18, 0xcd,
            // 0xeb, 0xe0, 0x15, 0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x87, 0xf3, 0x64, 0x2f, 0xf9,
            // 0xbf, 0x9a, 0xea, 0xcf, 0x55, 0x42, 0x0f, 0x56, 0xb9, 0x13, 0x51, 0x74, 0xae, 0x91,
            // 0xab, 0x5e, 0x3e, 0x5d, 0x6e, 0x97, 0xa8, 0x19, 0x01, 0xbe, 0x45, 0x72, 0xe8, 0x12,
            // 0x00, 0x18, 0xcd, 0xeb, 0xe0, 0x15, 0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x0e, 0x2e,
            // 0xe1, 0x9a, 0x6b, 0xf7, 0x56, 0x2a, 0xb6, 0xb1, 0x5c, 0x44, 0x8d, 0xd8, 0xec, 0x6a,
            // 0xf6, 0x3b, 0xe6, 0x4c, 0x73, 0x97, 0x08, 0x85, 0xef, 0x53, 0xed, 0x5f, 0x65, 0x36,
            // 0x84, 0xaf, 0x12, 0x00, 0x18, 0x84, 0xf2, 0xe8, 0x09, 0x0a, 0x27, 0x08, 0x02, 0x18,
            // 0x88, 0xc1, 0xa8, 0x35, 0x20, 0x80, 0x80, 0xe0, 0x15, 0x20, 0x80, 0x80, 0xe0, 0x15,
            // 0x20, 0x88, 0xc1, 0xe8, 0x09, 0x38, 0xa4, 0x83, 0x02, 0x42, 0x0b, 0x08, 0xa4, 0xd7,
            // 0xb9, 0xf6, 0x05, 0x15, 0x70, 0x94, 0x00, 0x00,
            //
            // this latter is go-ipfs 0.5 add with default options of linux-5.6.14.tar.xz
            0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x38, 0x22, 0x56, 0x0f, 0x94, 0x5f, 0xd3, 0xc7,
            0x45, 0x22, 0xde, 0x34, 0x48, 0x51, 0x2a, 0x7e, 0x45, 0xcb, 0x53, 0xf0, 0xa9, 0xa1,
            0xe1, 0x21, 0x61, 0xda, 0x46, 0x67, 0x53, 0x1e, 0xc1, 0x2e, 0x12, 0x00, 0x18, 0xae,
            0xd4, 0xe0, 0x15, 0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x85, 0x94, 0xeb, 0x4d, 0xd5,
            0xd6, 0x7e, 0x57, 0x3d, 0x50, 0x6c, 0xd9, 0x50, 0xac, 0x59, 0x86, 0x3b, 0x9a, 0xfb,
            0x02, 0x4a, 0x59, 0x0d, 0x7f, 0xe4, 0x9b, 0x42, 0xfb, 0xcb, 0x44, 0xaf, 0x43, 0x12,
            0x00, 0x18, 0xae, 0xd4, 0xe0, 0x15, 0x12, 0x2b, 0x0a, 0x22, 0x12, 0x20, 0x74, 0x5a,
            0x70, 0xb6, 0xcd, 0x7e, 0xc3, 0xe4, 0x6d, 0x16, 0xfb, 0x15, 0xb5, 0xe1, 0xe5, 0xdb,
            0x25, 0x6f, 0x6a, 0x7a, 0x52, 0xd0, 0xb3, 0x59, 0xf8, 0xf4, 0x9b, 0x24, 0x26, 0x65,
            0xe1, 0x7b, 0x12, 0x00, 0x18, 0xb4, 0xe7, 0xe8, 0x09, 0x0a, 0x16, 0x08, 0x02, 0x18,
            0x88, 0xc1, 0xa8, 0x35, 0x20, 0x80, 0x80, 0xe0, 0x15, 0x20, 0x80, 0x80, 0xe0, 0x15,
            0x20, 0x88, 0xc1, 0xe8, 0x09,
        ];

        let digest = multihash::Sha2_256::digest(input);
        println!("source: {}", multibase::Base::Base58Btc.encode(&digest));
        println!("source: {}", cid::Cid::new_v0(digest).unwrap());

        struct HexOnly<T>(T);

        impl<T> fmt::Debug for HexOnly<T>
        where
            T: std::convert::AsRef<[u8]>,
        {
            fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
                let inner = self.0.as_ref();
                write!(fmt, "({}: ", inner.len())?;
                for b in inner {
                    write!(fmt, "{:02x}", b)?
                }
                write!(fmt, ")")
            }
        }

        impl<T> fmt::Display for HexOnly<T>
        where
            T: std::convert::AsRef<[u8]>,
        {
            fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in self.0.as_ref() {
                    write!(fmt, "{:02x}", b)?
                }
                Ok(())
            }
        }

        let flat = FlatUnixFs::try_from(&input[..]).unwrap();

        let mut expected_ranges = vec![
            0..45_613_056,
            45_613_056..91_226_112,
            91_226_112..111_812_744,
        ];

        // 500_000...800_000

        expected_ranges.reverse();

        // multibase notes: using multibase::encode adds the multibase prefix, calling the bases
        // directly does not.

        for (link, range) in flat.range_links() {
            println!(
                "{:>12}..{:<12} {:?}",
                range.start,
                range.end,
                link.Hash
                    .map(|h| multibase::Base::Base32Upper.encode(h.as_ref())),
                // link.Hash.map(|h| multibase::Base::Base58Btc.encode(h.as_ref())),
                // link.Hash.map(|h| cid::Cid::try_from(h.as_ref()).unwrap().to_string()),
            );
            assert_eq!(Some(range), expected_ranges.pop());
            assert_eq!(link.Name, Some(Cow::Borrowed("")));
        }
    }
}
