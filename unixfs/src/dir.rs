use crate::pb::{FlatUnixFs, PBLink, PBNode, ParsingFailed, UnixFsType};
use crate::{InvalidCidInLink, UnexpectedNodeType};
use cid::Cid;
use core::convert::TryFrom;
use core::fmt;

mod sharded_lookup;
pub use sharded_lookup::{Cache, LookupError, ShardError, ShardedLookup};

mod directory;
pub(crate) use directory::{check_directory_supported, UnexpectedDirectoryProperties};

/// Directory tree builder.
pub mod builder;

pub(crate) fn check_hamtshard_supported(
    mut flat: FlatUnixFs<'_>,
) -> Result<FlatUnixFs<'_>, ShardError> {
    ShardedLookup::check_supported(&mut flat)?;
    Ok(flat)
}

/// Resolves a single path segment on `dag-pb` or UnixFS directories (normal, sharded).
///
/// The third parameter can always be substituted with a None but when repeatedly resolving over
/// multiple path segments, it can be used to cache the work queue used to avoid re-allocating it
/// between the steps.
///
/// Returns on success either a walker which can be used to traverse additional links searching for
/// the link, or the resolved link once it has been found or NotFound when it cannot be found.
///
/// # Note
///
/// The returned walker by default borrows the needle but it can be transformed into owned walker
/// with `ShardedLookup::with_owned_needle` which will allow moving it between tasks and boundaries.
pub fn resolve<'needle>(
    block: &[u8],
    needle: &'needle str,
    cache: &mut Option<Cache>,
) -> Result<MaybeResolved<'needle>, ResolveError> {
    let links = match FlatUnixFs::try_parse(block) {
        Ok(hamt) if hamt.data.Type == UnixFsType::HAMTShard => {
            return Ok(ShardedLookup::lookup_or_start(hamt, needle, cache)?)
        }
        Ok(flat) if flat.data.Type == UnixFsType::Directory => {
            check_directory_supported(flat)?.links
        }
        Err(ParsingFailed::InvalidUnixFs(_, PBNode { Links: links, .. }))
        | Err(ParsingFailed::NoData(PBNode { Links: links, .. })) => links,
        Ok(other) => {
            // go-ipfs does not resolve links under File, probably it's not supposed to work on
            // anything else then; returning NotFound would be correct, but perhaps it's even more
            // correct to return that we don't support this
            return Err(ResolveError::UnexpectedType(other.data.Type.into()));
        }
        Err(ParsingFailed::InvalidDagPb(e)) => return Err(ResolveError::Read(e)),
    };

    let mut matching = links.into_iter().enumerate().filter_map(|(i, link)| {
        match link.Name.as_deref().unwrap_or_default() {
            x if x == needle => Some((i, link)),
            _ => None,
        }
    });

    let first = matching.next();

    if let Some((i, first)) = first {
        let first = try_convert_cid(i, first)?;
        match matching.next() {
            Some((j, second)) => Err(MultipleMatchingLinks::from(((i, first), (j, second))).into()),
            None => Ok(MaybeResolved::Found(first)),
        }
    } else {
        Ok(MaybeResolved::NotFound)
    }
}

fn try_convert_cid(nth: usize, link: PBLink<'_>) -> Result<Cid, InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    Cid::try_from(hash).map_err(|e| InvalidCidInLink::from((nth, link, e)))
}

/// Resolving result type for the successful cases.
#[derive(Debug)]
pub enum MaybeResolved<'needle> {
    /// Link was found for the given segment.
    Found(Cid),
    /// The block presented to `resolve` was a HAMT sharded directory and other blocks need to be
    /// read in order to find the link. `ShardedLookup` will handle the lookup and navigation
    /// over the shards.
    NeedToLoadMore(ShardedLookup<'needle>),
    /// The segment could not be found.
    NotFound,
}

/// Resolving can fail similarly as with `ShardedLookup::continue_walk` but in addition to sharded
/// cases, there can be unexpected directories.
#[derive(Debug)]
pub enum ResolveError {
    /// The target block was a UnixFs node that doesn't support resolving, e.g. a file.
    UnexpectedType(UnexpectedNodeType),
    /// A directory had unsupported properties. These are not encountered during walking sharded
    /// directories.
    UnexpectedDirProperties(UnexpectedDirectoryProperties),
    /// Failed to read the block as a dag-pb node. Failure to read an inner UnixFS node is ignored
    /// and links of the outer dag-pb are processed.
    Read(quick_protobuf::Error),
    /// Lookup errors.
    Lookup(LookupError),
}

impl From<UnexpectedDirectoryProperties> for ResolveError {
    fn from(e: UnexpectedDirectoryProperties) -> Self {
        ResolveError::UnexpectedDirProperties(e)
    }
}

impl fmt::Display for ResolveError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ResolveError::*;
        match self {
            UnexpectedType(ut) => write!(fmt, "unexpected type for UnixFs: {:?}", ut),
            UnexpectedDirProperties(udp) => write!(fmt, "unexpected directory properties: {}", udp),
            Read(e) => write!(fmt, "parsing failed: {}", e),
            Lookup(e) => write!(fmt, "{}", e),
        }
    }
}

impl std::error::Error for ResolveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use ResolveError::*;
        match self {
            Read(e) => Some(e),
            Lookup(LookupError::Read(Some(e))) => Some(e),
            _ => None,
        }
    }
}

impl From<InvalidCidInLink> for ResolveError {
    fn from(e: InvalidCidInLink) -> ResolveError {
        ResolveError::Lookup(e.into())
    }
}

impl From<MultipleMatchingLinks> for ResolveError {
    fn from(e: MultipleMatchingLinks) -> ResolveError {
        ResolveError::Lookup(e.into())
    }
}

impl From<ShardError> for ResolveError {
    fn from(e: ShardError) -> ResolveError {
        ResolveError::Lookup(e.into())
    }
}

impl From<LookupError> for ResolveError {
    fn from(e: LookupError) -> ResolveError {
        ResolveError::Lookup(e)
    }
}

/// Multiple matching links were found: **at least two**.
#[derive(Debug)]
pub enum MultipleMatchingLinks {
    /// Two valid links were found
    Two {
        /// The first link and its index in the links
        first: (usize, Cid),
        /// The second link and its index in the links
        second: (usize, Cid),
    },
    /// Two links were matched but one of them could not be converted.
    OneValid {
        /// The first link and its index in the links
        first: (usize, Cid),
        /// The failure to parse the other link
        second: InvalidCidInLink,
    },
}

impl<'a> From<((usize, Cid), (usize, PBLink<'a>))> for MultipleMatchingLinks {
    fn from(
        ((i, first), (j, second)): ((usize, Cid), (usize, PBLink<'a>)),
    ) -> MultipleMatchingLinks {
        match try_convert_cid(j, second) {
            Ok(second) => MultipleMatchingLinks::Two {
                first: (i, first),
                second: (j, second),
            },
            Err(e) => MultipleMatchingLinks::OneValid {
                first: (i, first),
                second: e,
            },
        }
    }
}

impl MultipleMatchingLinks {
    /// Takes the first link, ignoring the other(s).
    pub fn into_inner(self) -> Cid {
        use MultipleMatchingLinks::*;
        match self {
            Two { first, .. } | OneValid { first, .. } => first.1,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{resolve, MaybeResolved};
    use crate::test_support::FakeBlockstore;
    use cid::Cid;
    use core::convert::TryFrom;
    use hex_literal::hex;

    #[test]
    fn resolve_paths_from_plain_dagpb() {
        let payload = hex!("12330a2212206aad27d7e2fc815cd15bf679535062565dc927a831547281fc0af9e5d7e67c74120b6166726963616e2e747874180812340a221220fd36ac5279964db0cba8f7fa45f8c4c44ef5e2ff55da85936a378c96c9c63204120c616d6572696361732e747874180812360a2212207564c20415869d77a8a40ca68a9158e397dd48bdff1325cdb23c5bcd181acd17120e6175737472616c69616e2e7478741808");

        assert!(
            crate::dagpb::node_data(&payload).unwrap().is_none(),
            "this payload has no data field"
        );

        let segments = [
            (
                "african.txt",
                Some("QmVX54jfjB8eRxLVxyQSod6b1FyDh7mR4mQie9j97i2Qk3"),
            ),
            (
                "americas.txt",
                Some("QmfP6D9bRV4FEYDL4EHZtZG58kDwDfnzmyjuyK5d1pvzbM"),
            ),
            (
                "australian.txt",
                Some("QmWEuXAjUGyndgr4MKqMBgzMW36XgPgvitt2jsXgtuc7JE"),
            ),
            ("not found", None),
            // this is not a hamt shard
            ("01african.txt", None),
        ];

        let mut cache = None;

        for (segment, link) in &segments {
            let target = link.map(|link| Cid::try_from(link).unwrap());

            let res = resolve(&payload[..], segment, &mut cache);

            match res {
                Ok(MaybeResolved::Found(cid)) => assert_eq!(Some(cid), target),
                Ok(MaybeResolved::NotFound) => {
                    assert!(target.is_none(), "should not have found {:?}", segment)
                }
                x => panic!("{:?}", x),
            }
        }
    }

    #[test]
    fn errors_with_file() {
        let payload = hex!("0a130802120d666f6f6261720a666f6f626172180d");
        // MaybeResolved::NotFound would be a possible answer as well, but this perhaps highlights
        // that we dont know how to resolve through this
        resolve(&payload[..], "anything", &mut None).unwrap_err();
    }

    #[test]
    fn sharded_directory_linking_to_non_sharded() {
        // created this test case out of doubt that we could fail a traversal as ShardedLookup
        // expects the linked cids to be hamt shards. However that cannot happen as we only resolve
        // a single step.
        let blocks = FakeBlockstore::with_fixtures();

        let block = blocks.get_by_str("QmQXUANxYGpkwMTWQUdZBPx9jqfFP7acNgL4FHRWkndKCe");

        let next = match resolve(&block[..], "non_sharded_dir", &mut None).unwrap() {
            MaybeResolved::Found(cid) => cid,
            x => unreachable!("{:?}", x),
        };

        let block = blocks.get_by_cid(&next);

        let next = match resolve(&block[..], "foobar", &mut None).unwrap() {
            MaybeResolved::Found(cid) => cid,
            x => unreachable!("{:?}", x),
        };

        assert_eq!(
            &next.to_string(),
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"
        );
    }
}
