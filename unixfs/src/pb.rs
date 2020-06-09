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

impl std::error::Error for UnixFsReadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use UnixFsReadFailed::*;

        match self {
            InvalidDagPb(e) => Some(e),
            InvalidUnixFs(e) => Some(e),
            NoData => None,
        }
    }
}

// This has been aliased as UnixFs<'a>
impl<'a> TryFrom<&'a merkledag::PBNode<'a>> for unixfs::Data<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(node: &'a merkledag::PBNode<'a>) -> Result<Self, Self::Error> {
        UnixFs::try_from(node.Data.as_ref().map(|a| a.as_ref()))
    }
}

// This has been aliased as UnixFs<'a>
impl<'a> TryFrom<Option<&'a [u8]>> for unixfs::Data<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(data: Option<&'a [u8]>) -> Result<Self, Self::Error> {
        use quick_protobuf::{BytesReader, MessageRead};

        let data = data.ok_or(UnixFsReadFailed::NoData)?;
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
#[derive(Debug)]
pub(crate) struct FlatUnixFs<'a> {
    pub(crate) links: Vec<PBLink<'a>>,
    pub(crate) data: UnixFs<'a>,
}

impl<'a> FlatUnixFs<'a> {
    pub(crate) fn try_parse(
        data: &'a [u8],
    ) -> Result<Self, (UnixFsReadFailed, Option<PBNode<'a>>)> {
        let node = merkledag::PBNode::try_from(data)
            .map_err(|e| (UnixFsReadFailed::InvalidDagPb(e), None))?;

        let data = match node.Data {
            Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => Some(bytes),
            Some(Cow::Owned(_)) => unreachable!(),
            Some(Cow::Borrowed(_)) | None => return Err((UnixFsReadFailed::NoData, None)),
        };

        match UnixFs::try_from(data) {
            Ok(data) => Ok(FlatUnixFs {
                links: node.Links,
                data,
            }),
            Err(e) => Err((e, Some(node))),
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for FlatUnixFs<'a> {
    type Error = UnixFsReadFailed;

    fn try_from(data: &'a [u8]) -> Result<Self, Self::Error> {
        Self::try_parse(data).map_err(|(e, _node)| e)
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
        self.inner.next().map(|(link, blocksize)| {
            let returned_base = self.base;
            self.base += blocksize;
            (link, returned_base..(returned_base + blocksize))
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod test {

    use super::{FlatUnixFs, PBNode, UnixFs, UnixFsType};
    use hex_literal::hex;
    use std::borrow::Cow;
    use std::convert::TryFrom;

    #[test]
    fn parse_content() {
        use quick_protobuf::{BytesReader, MessageRead};
        let input = hex!("0a0d08021207636f6e74656e741807");

        let mut reader = BytesReader::from_bytes(&input);
        let dagnode =
            PBNode::from_reader(&mut reader, &input).expect("parse outer merkledag::PBNode");
        assert!(dagnode.Links.is_empty());

        let unixfs_data = UnixFs::try_from(&dagnode).expect("parse inner unixfs::Data");
        assert_eq!(unixfs_data.Type, UnixFsType::File);
        assert_eq!(unixfs_data.Data, Some(Cow::Borrowed(&b"content"[..])));
        assert_eq!(unixfs_data.filesize, Some(7));
        println!("{:?}", unixfs_data);
    }

    #[test]
    fn linux_tarxz_range_links() {
        let input = hex!("122b0a2212203822560f945fd3c74522de3448512a7e45cb53f0a9a1e12161da4667531ec12e120018aed4e015122b0a2212208594eb4dd5d67e573d506cd950ac59863b9afb024a590d7fe49b42fbcb44af43120018aed4e015122b0a221220745a70b6cd7ec3e46d16fb15b5e1e5db256f6a7a52d0b359f8f49b242665e17b120018b4e7e8090a1608021888c1a835208080e015208080e0152088c1e809");

        let flat = FlatUnixFs::try_from(&input[..]).unwrap();

        let mut expected_ranges = vec![
            0..45_613_056,
            45_613_056..91_226_112,
            91_226_112..111_812_744,
        ];

        expected_ranges.reverse();

        for (link, range) in flat.range_links() {
            assert_eq!(link.Name, Some(Cow::Borrowed("")));
            // Tsize is the subtree size, which must always be larger than the file segments
            // because of encoding
            assert!(link.Tsize >= Some(range.end - range.start));
            assert_eq!(Some(range), expected_ranges.pop());
        }
    }
}
