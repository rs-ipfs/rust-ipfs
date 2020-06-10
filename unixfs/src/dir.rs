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
    match FlatUnixFs::try_parse(block) {
        Ok(hamt) if hamt.data.Type == UnixFsType::HAMTShard => {
            // TODO: make sure these are as expected
            eprintln!(
                "filesize={:?}, blocksizes={}, hash_type: {:?}, fanout: {:?}",
                hamt.data.filesize,
                hamt.data.blocksizes.len(),
                hamt.data.hashType,
                hamt.data.fanout
            );

            // not sure if we need to concern ourselves with fanout?
            if hamt.data.fanout != Some(256) {
                todo!()
                //return Err(ResolveError::Unsupported(HamtError::Fanout(hamt.data.fanout)));
            }

            if hamt.data.filesize.is_some() || !hamt.data.blocksizes.is_empty() {
                todo!()
            }

            let mut links = cache.take().map(|c| c.buffer).unwrap_or_default();

            let found = ShardedLookup::partition(
                hamt.links.into_iter(),
                needle,
                &mut links)?;

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

            eprintln!("Searching normal directory");

            Ok(search_normal_links(flat.links.into_iter().enumerate(), needle)?.into())
        }
        Err((_, Some(PBNode { Links: links, .. }))) => {
            eprintln!("Searching outer dag-pb");
            Ok(search_normal_links(links.into_iter().enumerate(), needle)?.into())
        }
        Ok(_other) => {
            eprintln!("Not finding anything under: {:?}", _other.data.Type);
            // go-ipfs does not resolve links under File, probably it's not supposed to work on
            // anything other then
            Ok(MaybeResolved::NotFound)
        }
        Err((UnixFsReadFailed::InvalidDagPb(e), None)) => {
            return Err(ResolveError::Walk(WalkError::Read(e)))
        }
        // FIXME: add an additional error type to handle this case..
        Err((UnixFsReadFailed::NoData, _)) | Err((UnixFsReadFailed::InvalidUnixFs(_), _)) => {
            unreachable!("Cannot have NoData without recovered outer dag-pb node")
        }
    }
}

fn search_normal_links<'a, 'b>(
    links: impl Iterator<Item = (usize, PBLink<'a>)>,
    needle: &'b str,
) -> Result<Option<Cid>, ResolveError> {
    let matching = links
        .filter_map(|(i, link)| match link.Name.as_deref().unwrap_or_default() {
            x if x == needle => Some((i, Cow::Borrowed(link.Hash.unwrap_borrowed_or_empty()))),
            _ => None,
        });

    process_results(matching)
}

fn process_results<'a>(
    mut matching: impl Iterator<Item = (usize, Cow<'a, [u8]>)>,
) -> Result<Option<Cid>, ResolveError> {
    let first = matching.next();

    if let Some((i, first)) = first {
        let first = try_convert_cid(i, first.as_ref())?;
        match matching.next() {
            Some((j, Cow::Borrowed(second))) => Err(MultipleMatchingLinks::from(((i, first), (j, second))).into()),
            Some((_, Cow::Owned(_))) => unreachable!("never taken ownership of"),
            None => Ok(Some(first)),
        }
    } else {
        Ok(None)
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

        let hamt = match FlatUnixFs::try_from(next) {
            Ok(hamt) if hamt.data.Type == UnixFsType::HAMTShard => hamt,
            Ok(other) => return Err(WalkError::UnexpectedBucketType(other.data.Type.into())),
            Err(UnixFsReadFailed::InvalidDagPb(e)) | Err(UnixFsReadFailed::InvalidUnixFs(e)) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(WalkError::Read(e))
            }
            Err(UnixFsReadFailed::NoData) => {
                *cache = Some(Cache { buffer: self.links });
                return Err(WalkError::EmptyDagPb)
            }
        };

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

    /// Partition the original links per their kind if the link:
    ///
    ///  - matches the needle uniquely, it will be returned as Some(cid)
    ///  - is a bucket, it is pushed back to the work
    pub(crate) fn partition<'a>(iter: impl Iterator<Item = PBLink<'a>>, needle: &str, work: &mut VecDeque<Cid>) -> Result<Option<Cid>, WalkError> {
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
    Multiple(MultipleMatchingLinks),
    InvalidCidInLink(InvalidCidInLink),
    UnexpectedDirProperties {
        filesize: Option<u64>,
        blocksizes: Vec<u64>,
        hash_type: Option<u64>,
        fanout: Option<u64>,
    },
    Walk(WalkError),
}

impl From<InvalidCidInLink> for ResolveError {
    fn from(e: InvalidCidInLink) -> ResolveError {
        ResolveError::InvalidCidInLink(e)
    }
}

impl From<MultipleMatchingLinks> for ResolveError {
    fn from(e: MultipleMatchingLinks) -> ResolveError {
        ResolveError::Multiple(e)
    }
}

impl From<WalkError> for ResolveError {
    fn from(e: WalkError) -> ResolveError {
        // FIXME: this makes no sense
        ResolveError::Walk(e)
    }
}

#[derive(Debug)]
pub struct InvalidCidInLink {
    pub nth: usize,
    pub raw: Vec<u8>,
    pub source: cid::Error,
    /// This is to deny creating these outside of the crate
    hidden: (),
}

#[derive(Debug)]
pub enum MultipleMatchingLinks {
    Two {
        first: (usize, Cid),
        second: (usize, Cid),
    },
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
    InvalidCidInLink(InvalidCidInLink),
    Multiple(MultipleMatchingLinks),
    UnexpectedBucketType(i32),
    Read(quick_protobuf::Error),
    EmptyDagPb,
}

impl From<MultipleMatchingLinks> for WalkError {
    fn from(e: MultipleMatchingLinks) -> Self {
        WalkError::Multiple(e)
    }
}

impl From<InvalidCidInLink> for WalkError {
    fn from(e: InvalidCidInLink) -> Self {
        WalkError::InvalidCidInLink(e)
    }
}

impl fmt::Display for WalkError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use WalkError::*;

        match self {
            InvalidCidInLink(e) => write!(fmt, "Invalid link: {:?}", e),
            Multiple(e) => write!(fmt, "Multiple matching links found: {:?}", e),
            UnexpectedBucketType(t) => write!(
                fmt,
                "unexpected type for HAMT bucket: {} or {:?}",
                t,
                UnixFsType::from(*t)
            ),
            Read(e) => write!(
                fmt,
                "failed to parse the block as unixfs or dag-pb node: {}",
                e
            ),
            EmptyDagPb => write!(fmt, "HAMTDirectory not found in empty dag-pb node"),
        }
    }
}

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use WalkError::*;
        match self {
            Read(e) => Some(e),
            _ => None,
        }
    }
}
