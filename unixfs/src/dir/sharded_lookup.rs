use super::{try_convert_cid, MaybeResolved, MultipleMatchingLinks, ResolveError};
use crate::pb::{FlatUnixFs, PBLink, ParsingFailed, UnixFsType};
use crate::InvalidCidInLink;
use cid::Cid;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt;

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
            Err(ParsingFailed::InvalidDagPb(e)) | Err(ParsingFailed::InvalidUnixFs(e, _)) => {
                *cache = Some(self.links.into());
                return Err(LookupError::Read(Some(e)));
            }
            Err(ParsingFailed::NoData(_)) => {
                *cache = Some(self.links.into());
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

    /// Finds or starts a lookup of multiple buckets.
    ///
    /// Returns the found link, the definitive negative or means to continue traversal.
    pub(crate) fn lookup_or_start(
        mut hamt: FlatUnixFs<'_>,
        needle: &'needle str,
        cache: &mut Option<Cache>,
    ) -> Result<MaybeResolved<'needle>, LookupError> {
        Self::check_supported(&mut hamt)?;

        let mut links = cache.take().map(|c| c.buffer).unwrap_or_default();

        let found = Self::partition(hamt.links.into_iter(), needle, &mut links)?;

        if let Some(cid) = found {
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
        }
    }

    /// Takes the validated object as mutable reference to move data out of it in case of error.
    ///
    /// Returns an error if we don't support the properties on the HAMTShard typed node
    fn check_supported(hamt: &mut FlatUnixFs<'_>) -> Result<(), ShardError> {
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
    fn partition<'a>(
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
