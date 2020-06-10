use crate::file::UnwrapBorrowedExt;
use crate::pb::{FlatUnixFs, PBLink, PBNode, UnixFsReadFailed, UnixFsType};
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

            let found = ShardedLookup::partition(
                hamt.links.into_iter(),
                needle,
                &mut links)?;

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
        Err((_, Some(PBNode { Links: links, .. }))) => {
            links
        }
        Ok(_other) => {
            // go-ipfs does not resolve links under File, probably it's not supposed to work on
            // anything other then
            return Ok(MaybeResolved::NotFound)
        }
        Err((UnixFsReadFailed::InvalidDagPb(e), None)) => {
            return Err(ResolveError::Read(e))
        }
        // FIXME: add an additional error type to handle this case..
        Err((UnixFsReadFailed::NoData, _)) | Err((UnixFsReadFailed::InvalidUnixFs(_), _)) => {
            unreachable!("Cannot have NoData without recovered outer dag-pb node")
        }
    };

    let mut matching = links.into_iter().enumerate()
        .filter_map(|(i, link)| match link.Name.as_deref().unwrap_or_default() {
            x if x == needle => Some((i, Cow::Borrowed(link.Hash.unwrap_borrowed_or_empty()))),
            _ => None,
        });

    let first = matching.next();

    if let Some((i, first)) = first {
        let first = try_convert_cid(i, first.as_ref())?;
        match matching.next() {
            Some((j, Cow::Borrowed(second))) => Err(MultipleMatchingLinks::from(((i, first), (j, second))).into()),
            Some((_, Cow::Owned(_))) => unreachable!("never taken ownership of"),
            None => Ok(MaybeResolved::Found(first)),
        }
    } else {
        Ok(MaybeResolved::NotFound)
    }
}

fn try_convert_cid(nth: usize, hash: &[u8]) -> Result<Cid, InvalidCidInLink> {
    Cid::try_from(hash).map_err(|e| InvalidCidInLink {
        nth,
        raw: hash.to_vec(),
        source: e,
        hidden: (),
    })
}

pub enum MaybeResolved<'needle> {
    NeedToLoadMore(ShardedLookup<'needle>),
    Found(Cid),
    NotFound,
}

pub struct Cache {
    buffer: VecDeque<Cid>,
}

impl From<VecDeque<Cid>> for Cache {
    fn from(mut buffer: VecDeque<Cid>) -> Self {
        buffer.clear();
        Cache {
            buffer,
        }
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

impl<'needle> ShardedLookup<'needle> {
    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid>) {
        let mut iter = self.links.iter();
        let first = iter.next().expect("Already validated there are links");
        (first, iter)
    }

    /// Continues the walk in the DAG of HAMT buckets searching for the original `needle`.
    pub fn continue_walk(mut self, next: &[u8], cache: &mut Option<Cache>) -> Result<MaybeResolved<'needle>, WalkError> {
        // just to make sure not to mess this up
        debug_assert_eq!(Some(self.pending_links().0), self.links.front());

        self.links
            .pop_front()
            .expect("Already validated there are links");

        let mut hamt = match FlatUnixFs::try_from(next) {
            Ok(hamt) if hamt.data.Type == UnixFsType::HAMTShard => hamt,
            Ok(other) => return Err(WalkError::UnexpectedBucketType(other.data.Type.into())),
            Err(UnixFsReadFailed::InvalidDagPb(e)) | Err(UnixFsReadFailed::InvalidUnixFs(e)) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(WalkError::Read(Some(e)))
            }
            Err(UnixFsReadFailed::NoData) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(WalkError::Read(None))
            }
        };

        Self::check_supported(&mut hamt)?;

        let found = Self::partition(
            hamt.links.into_iter(),
            self.needle.as_ref(),
            &mut self.links)?;

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
        work: &mut VecDeque<Cid>) -> Result<Option<Cid>, PartitioningError> {
        let mut found = None;

        for (i, link) in iter.enumerate() {
            let name = link.Name.as_deref().unwrap_or_default();

            if name.len() > 2 && &name[2..] == needle {
                let hash = link.Hash.as_deref().unwrap_or_default();

                if let Some(first) = found.take() {
                    return Err(MultipleMatchingLinks::from((first, (i, hash))).into());
                } else {
                    found = Some((i, try_convert_cid(i, hash)?));
                }
            } else if name.len() == 2 {
                // the magic number of two comes from the fanout (256) probably
                let cid = try_convert_cid(i, link.Hash.unwrap_borrowed_or_empty())?;
                work.push_back(cid);
            } else {
                // no match, not interesting for us
            }
        }

        Ok(found.map(|(_, cid)| cid))
    }
}

#[derive(Debug)]
pub enum ResolveError {
    /// Two or more suitable links were found.
    Multiple(MultipleMatchingLinks),
    /// A hash in a link could not be converted to a Cid.
    InvalidCid(InvalidCidInLink),
    /// A directory had unsupported properties. These are not encountered during walking sharded
    /// directories.
    UnexpectedDirProperties {
        filesize: Option<u64>,
        blocksizes: Vec<u64>,
        hash_type: Option<u64>,
        fanout: Option<u64>,
    },
    /// Read an HAMTSharded directory which had unexpected properties
    Shard(ShardError),
    /// Failed to read the block as an dag-pb node. Failure to read an inner UnixFS node is ignored
    /// and links of the outer dag-pb are processed.
    Read(quick_protobuf::Error),
}

impl From<InvalidCidInLink> for ResolveError {
    fn from(e: InvalidCidInLink) -> ResolveError {
        ResolveError::InvalidCid(e)
    }
}

impl From<MultipleMatchingLinks> for ResolveError {
    fn from(e: MultipleMatchingLinks) -> ResolveError {
        ResolveError::Multiple(e)
    }
}

impl From<ShardError> for ResolveError {
    fn from(e: ShardError) -> ResolveError {
        ResolveError::Shard(e)
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
        use PartitioningError::*;
        match e {
            Multiple(m) => ResolveError::Multiple(m),
            InvalidCid(e) => ResolveError::InvalidCid(e),
        }
    }
}

impl From<PartitioningError> for WalkError {
    fn from(e: PartitioningError) -> WalkError {
        use PartitioningError::*;
        match e {
            Multiple(m) => WalkError::Multiple(m),
            InvalidCid(e) => WalkError::InvalidCid(e),
        }
    }
}

/// Shard does not fit into expectations.
#[derive(Debug)]
pub enum ShardError {
    /// Encountered an HAMT sharded directory which had an unsupported configuration.
    UnsupportedProperties {
        hash_type: Option<u64>,
        fanout: Option<u64>,
    },
    /// Encountered an HAMT sharded directory which had a unexpected properties.
    UnexpectedProperties {
        filesize: Option<u64>,
        blocksizes: Vec<u64>,
    }
}

impl fmt::Display for ShardError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ShardError::*;
        match self {
            UnsupportedProperties { hash_type, fanout } => write!(fmt, "unsupported HAMTShard properties: hash_type={:?}, fanout={:?}", hash_type, fanout),
            UnexpectedProperties { filesize, blocksizes } => write!(fmt, "unexpected HAMTShard properties: filesize=({:?}), {} blocksizes", filesize, blocksizes.len()),
        }
    }
}

impl std::error::Error for ShardError {}

/// A link could not be transformed into a Cid.
#[derive(Debug)]
pub struct InvalidCidInLink {
    pub nth: usize,
    pub raw: Vec<u8>,
    pub source: cid::Error,
    /// This is to deny creating these outside of the crate
    hidden: (),
}

/// Multiple matching links were found: **at least two**.
#[derive(Debug)]
pub enum MultipleMatchingLinks {
    /// Two valid links were found
    Two {
        first: (usize, Cid),
        second: (usize, Cid),
    },
    /// Two matched links but the other could not be converted.
    OneValid {
        first: (usize, Cid),
        second: InvalidCidInLink,
    },
}

impl<'a> From<((usize, Cid), (usize, &'a [u8]))> for MultipleMatchingLinks {
    fn from(((i, first), (j, second)): ((usize, Cid), (usize, &'a [u8]))) -> MultipleMatchingLinks {
        match try_convert_cid(j, second) {
            Ok(second) => MultipleMatchingLinks::Two {
                first: (i, first),
                second: (j, second),
            },
            Err(e) => MultipleMatchingLinks::OneValid {
                first: (i, first),
                second: e,
            }
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

#[derive(Debug)]
pub enum WalkError {
    Multiple(MultipleMatchingLinks),
    InvalidCid(InvalidCidInLink),
    UnexpectedBucketType(i32),
    Shard(ShardError),
    Read(Option<quick_protobuf::Error>),
}

impl From<MultipleMatchingLinks> for WalkError {
    fn from(e: MultipleMatchingLinks) -> Self {
        WalkError::Multiple(e)
    }
}

impl From<InvalidCidInLink> for WalkError {
    fn from(e: InvalidCidInLink) -> Self {
        WalkError::InvalidCid(e)
    }
}

impl From<ShardError> for WalkError {
    fn from(e: ShardError) -> Self {
        WalkError::Shard(e)
    }
}

impl fmt::Display for WalkError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use WalkError::*;

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

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use WalkError::*;
        match self {
            Read(Some(e)) => Some(e),
            _ => None,
        }
    }
}
