use crate::pb::{FlatUnixFs, PBLink, PBNode, UnixFsReadFailed, UnixFsType};
use crate::InvalidCidInLink;
use cid::Cid;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt;

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
        Ok(mut hamt) if hamt.data.Type == UnixFsType::HAMTShard => {
            ShardedLookup::check_supported(&mut hamt)?;

            let mut links = cache.take().map(|c| c.buffer).unwrap_or_default();

            let found = ShardedLookup::partition(hamt.links.into_iter(), needle, &mut links)?;

            return if let Some(cid) = found {
                *cache = Some(links.into());
                Ok(MaybeResolved::Found(cid))
            } else if links.is_empty() {
                *cache = Some(links.into());
                Ok(MaybeResolved::NotFound)
            } else {
                Ok(MaybeResolved::NeedToLoadMore(ShardedLookup {
                    links,
                    needle: Cow::Borrowed(needle),
                }))
            };
        }
        Ok(flat) if flat.data.Type == UnixFsType::Directory => {
            if flat.data.filesize.is_some()
                || !flat.data.blocksizes.is_empty()
                || flat.data.hashType.is_some()
                || flat.data.fanout.is_some()
            {
                return Err(ResolveError::UnexpectedDirProperties {
                    filesize: flat.data.filesize,
                    blocksizes: flat.data.blocksizes,
                    hash_type: flat.data.hashType,
                    fanout: flat.data.fanout,
                });
            }

            flat.links
        }
        Err((_, Some(PBNode { Links: links, .. }))) => links,
        Ok(_other) => {
            // go-ipfs does not resolve links under File, probably it's not supposed to work on
            // anything other then
            return Ok(MaybeResolved::NotFound);
        }
        Err((UnixFsReadFailed::InvalidDagPb(e), None)) => return Err(ResolveError::Read(e)),
        // FIXME: add an additional error type to handle this case..
        Err((UnixFsReadFailed::NoData, _)) | Err((UnixFsReadFailed::InvalidUnixFs(_), _)) => {
            unreachable!("Cannot have NoData without recovered outer dag-pb node")
        }
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

/// Cache of datastructures used while traversing. Reduces allocations when walking over multiple
/// path segments.
pub struct Cache {
    buffer: VecDeque<Cid>,
}

impl From<VecDeque<Cid>> for Cache {
    fn from(mut buffer: VecDeque<Cid>) -> Self {
        buffer.clear();
        Cache { buffer }
    }
}
impl fmt::Debug for Cache {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Cache {{ buffer: {} }}", self.buffer.capacity())
    }
}

impl From<Option<Cid>> for MaybeResolved<'static> {
    fn from(maybe: Option<Cid>) -> Self {
        if let Some(cid) = maybe {
            MaybeResolved::Found(cid)
        } else {
            MaybeResolved::NotFound
        }
    }
}

/// `ShardedLookup` can walk over multiple HAMT sharded directory nodes which allows multiple block
/// spanning directories.
pub struct ShardedLookup<'needle> {
    // TODO: tempted to put Vec<(Cid, &'data str)> here but maybe that can be the underlying
    // reader used for listing as well?
    // Need to have VecDeque to do BFS
    links: VecDeque<Cid>,
    // this will be tricky if we ever need to have a case-insensitive resolving *but* we can then
    // make a custom Cow type; important not to expose Cow in any API.
    needle: Cow<'needle, str>,
}

impl fmt::Debug for ShardedLookup<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "ShardedLookup {{ links: {}, needle: {:?} }}",
            self.links.len(),
            self.needle.as_ref(),
        )
    }
}

impl<'needle> ShardedLookup<'needle> {
    /// Returns the next pending link and an iterator over the rest.
    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid>) {
        let mut iter = self.links.iter();
        let first = iter.next().expect("Already validated there are links");
        (first, iter)
    }

    /// Continues the walk in the DAG of HAMT buckets searching for the original `needle`.
    pub fn continue_walk(
        mut self,
        next: &[u8],
        cache: &mut Option<Cache>,
    ) -> Result<MaybeResolved<'needle>, LookupError> {
        // just to make sure not to mess this up
        debug_assert_eq!(Some(self.pending_links().0), self.links.front());

        self.links
            .pop_front()
            .expect("Already validated there are links");

        let mut hamt = match FlatUnixFs::try_from(next) {
            Ok(hamt) if hamt.data.Type == UnixFsType::HAMTShard => hamt,
            Ok(other) => return Err(LookupError::UnexpectedBucketType(other.data.Type.into())),
            Err(UnixFsReadFailed::InvalidDagPb(e)) | Err(UnixFsReadFailed::InvalidUnixFs(e)) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(LookupError::Read(Some(e)));
            }
            Err(UnixFsReadFailed::NoData) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(LookupError::Read(None));
            }
        };

        Self::check_supported(&mut hamt)?;

        let found = Self::partition(
            hamt.links.into_iter(),
            self.needle.as_ref(),
            &mut self.links,
        )?;

        if let Some(cid) = found {
            *cache = Some(self.links.into());
            Ok(MaybeResolved::Found(cid))
        } else if self.links.is_empty() {
            *cache = Some(self.links.into());
            Ok(MaybeResolved::NotFound)
        } else {
            Ok(MaybeResolved::NeedToLoadMore(self))
        }
    }

    /// Transforms this `ShardedLookup` into an `ShardedLookup<'static>` by taking ownership of the
    /// needle we are trying to find.
    pub fn with_owned_needle(self) -> ShardedLookup<'static> {
        let ShardedLookup { links, needle } = self;
        let needle = Cow::Owned(needle.into_owned());
        ShardedLookup { links, needle }
    }

    /// Takes the validated object as mutable reference to move data out of it in case of error.
    ///
    /// Returns an error if we don't support the properties on the HAMTShard typed node
    pub(crate) fn check_supported(hamt: &mut FlatUnixFs<'_>) -> Result<(), ShardError> {
        assert_eq!(hamt.data.Type, UnixFsType::HAMTShard);

        if hamt.data.fanout != Some(256) || hamt.data.hashType != Some(34) {
            Err(ShardError::UnsupportedProperties {
                hash_type: hamt.data.hashType,
                fanout: hamt.data.fanout,
            })
        } else if hamt.data.filesize.is_some() || !hamt.data.blocksizes.is_empty() {
            Err(ShardError::UnexpectedProperties {
                filesize: hamt.data.filesize,
                blocksizes: std::mem::take(&mut hamt.data.blocksizes),
            })
        } else {
            Ok(())
        }
    }

    /// Partition the original links per their kind if the link:
    ///
    ///  - matches the needle uniquely, it will be returned as Some(cid)
    ///  - is a bucket, it is pushed back to the work
    pub(crate) fn partition<'a>(
        iter: impl Iterator<Item = PBLink<'a>>,
        needle: &str,
        work: &mut VecDeque<Cid>,
    ) -> Result<Option<Cid>, PartitioningError> {
        let mut found = None;

        for (i, link) in iter.enumerate() {
            let name = link.Name.as_deref().unwrap_or_default();

            if name.len() > 2 && &name[2..] == needle {
                if let Some(first) = found.take() {
                    return Err(MultipleMatchingLinks::from((first, (i, link))).into());
                } else {
                    found = Some((i, try_convert_cid(i, link)?));
                }
            } else if name.len() == 2 {
                // the magic number of two comes from the fanout (256) probably
                let cid = try_convert_cid(i, link)?;
                work.push_back(cid);
            } else {
                // no match, not interesting for us
            }
        }

        Ok(found.map(|(_, cid)| cid))
    }
}

/// Resolving can fail similarly as with `ShardedLookup::continue_walk` but in addition to sharded
/// cases, there can be unexpected directories.
#[derive(Debug)]
pub enum ResolveError {
    /// A directory had unsupported properties. These are not encountered during walking sharded
    /// directories.
    UnexpectedDirProperties {
        /// filesize is a property of Files
        filesize: Option<u64>,
        /// blocksizes is a property of Files
        blocksizes: Vec<u64>,
        /// hash_type is a property of HAMT Shards
        hash_type: Option<u64>,
        /// fanout is a property of HAMT shards
        fanout: Option<u64>,
    },
    /// Failed to read the block as an dag-pb node. Failure to read an inner UnixFS node is ignored
    /// and links of the outer dag-pb are processed.
    Read(quick_protobuf::Error),
    /// Lookup errors.
    Lookup(LookupError),
}

impl fmt::Display for ResolveError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ResolveError::*;
        match self {
            UnexpectedDirProperties { filesize, blocksizes, hash_type, fanout } => write!(
                fmt,
                "unexpected directory properties: filesize={:?}, {} blocksizes, hash_type={:?}, fanout={:?}",
                filesize,
                blocksizes.len(),
                hash_type,
                fanout
            ),
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

pub(crate) enum PartitioningError {
    Multiple(MultipleMatchingLinks),
    InvalidCid(InvalidCidInLink),
}

impl From<InvalidCidInLink> for PartitioningError {
    fn from(e: InvalidCidInLink) -> PartitioningError {
        PartitioningError::InvalidCid(e)
    }
}

impl From<MultipleMatchingLinks> for PartitioningError {
    fn from(e: MultipleMatchingLinks) -> PartitioningError {
        PartitioningError::Multiple(e)
    }
}

impl From<PartitioningError> for ResolveError {
    fn from(e: PartitioningError) -> ResolveError {
        ResolveError::Lookup(LookupError::from(e))
    }
}

impl From<PartitioningError> for LookupError {
    fn from(e: PartitioningError) -> LookupError {
        use PartitioningError::*;
        match e {
            Multiple(m) => LookupError::Multiple(m),
            InvalidCid(e) => LookupError::InvalidCid(e),
        }
    }
}

/// Shard does not fit into expectations.
#[derive(Debug)]
pub enum ShardError {
    /// Encountered an HAMT sharded directory which had an unsupported configuration.
    UnsupportedProperties {
        /// Unsupported multihash hash.
        hash_type: Option<u64>,
        /// Unsupported fanout value.
        fanout: Option<u64>,
    },
    /// Encountered an HAMT sharded directory which had a unexpected properties.
    UnexpectedProperties {
        /// Filesize is used with UnixFS files.
        filesize: Option<u64>,
        /// Blocksizes are in general used with UnixFS files.
        blocksizes: Vec<u64>,
    },
}

impl fmt::Display for ShardError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ShardError::*;
        match self {
            UnsupportedProperties { hash_type, fanout } => write!(
                fmt,
                "unsupported HAMTShard properties: hash_type={:?}, fanout={:?}",
                hash_type, fanout
            ),
            UnexpectedProperties {
                filesize,
                blocksizes,
            } => write!(
                fmt,
                "unexpected HAMTShard properties: filesize=({:?}), {} blocksizes",
                filesize,
                blocksizes.len()
            ),
        }
    }
}

impl std::error::Error for ShardError {}

/// Multiple matching links were found: **at least two**.
#[derive(Debug)]
pub enum MultipleMatchingLinks {
    /// Two valid links were found
    Two {
        /// The first link and it's index in the links
        first: (usize, Cid),
        /// The second link and it's index in the links
        second: (usize, Cid),
    },
    /// Two matched links but the other could not be converted.
    OneValid {
        /// The first link and it's index in the links
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

/// Errors which can occur when looking up a HAMTSharded directory.
#[derive(Debug)]
pub enum LookupError {
    /// Multiple matching links were found
    Multiple(MultipleMatchingLinks),
    /// Invalid Cid was matched
    InvalidCid(InvalidCidInLink),
    /// Unexpected HAMT shard bucket type
    UnexpectedBucketType(i32),
    /// Unsupported or unexpected property of the UnixFS node
    Shard(ShardError),
    /// Parsing failed or the inner dag-pb data was contained no bytes.
    Read(Option<quick_protobuf::Error>),
}

impl LookupError {
    /// Converts this HAMT lookup error to the more general ResolveError
    pub fn into_resolve_error(self) -> ResolveError {
        self.into()
    }
}

impl From<MultipleMatchingLinks> for LookupError {
    fn from(e: MultipleMatchingLinks) -> Self {
        LookupError::Multiple(e)
    }
}

impl From<InvalidCidInLink> for LookupError {
    fn from(e: InvalidCidInLink) -> Self {
        LookupError::InvalidCid(e)
    }
}

impl From<ShardError> for LookupError {
    fn from(e: ShardError) -> Self {
        LookupError::Shard(e)
    }
}

impl fmt::Display for LookupError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use LookupError::*;

        match self {
            InvalidCid(e) => write!(fmt, "Invalid link: {:?}", e),
            Multiple(e) => write!(fmt, "Multiple matching links found: {:?}", e),
            UnexpectedBucketType(t) => write!(
                fmt,
                "unexpected type for HAMT bucket: {} or {:?}",
                t,
                UnixFsType::from(*t)
            ),
            Shard(e) => write!(fmt, "{}", e),
            Read(Some(e)) => write!(
                fmt,
                "failed to parse the block as unixfs or dag-pb node: {}",
                e
            ),
            Read(None) => write!(fmt, "HAMTDirectory not found in empty dag-pb node"),
        }
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use LookupError::*;
        match self {
            Read(Some(e)) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::convert::TryFrom;
    use cid::Cid;
    use hex_literal::hex;
    use super::{resolve, MaybeResolved};

    #[test]
    fn resolve_paths() {
        let payload = hex!("12330a2212206aad27d7e2fc815cd15bf679535062565dc927a831547281fc0af9e5d7e67c74120b6166726963616e2e747874180812340a221220fd36ac5279964db0cba8f7fa45f8c4c44ef5e2ff55da85936a378c96c9c63204120c616d6572696361732e747874180812360a2212207564c20415869d77a8a40ca68a9158e397dd48bdff1325cdb23c5bcd181acd17120e6175737472616c69616e2e7478741808");

        let segments = [
            ("african.txt", "QmVX54jfjB8eRxLVxyQSod6b1FyDh7mR4mQie9j97i2Qk3"),
            ("americas.txt","QmfP6D9bRV4FEYDL4EHZtZG58kDwDfnzmyjuyK5d1pvzbM"),
            ("australian.txt", "QmWEuXAjUGyndgr4MKqMBgzMW36XgPgvitt2jsXgtuc7JE")
        ];

        let mut cache = None;

        for (segment, link) in &segments {

            let target = Cid::try_from(*link).unwrap();

            let res = resolve(&payload[..], segment, &mut cache);

            match res {
                Ok(MaybeResolved::Found(cid)) => assert_eq!(cid, target),
                x => panic!("{:?}", x),
            }
        }
    }
}
