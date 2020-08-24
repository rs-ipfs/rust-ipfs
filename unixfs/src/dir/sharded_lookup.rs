use super::{try_convert_cid, MaybeResolved, MultipleMatchingLinks, ResolveError};
use crate::pb::{FlatUnixFs, PBLink, ParsingFailed, UnixFsType};
use crate::{InvalidCidInLink, UnexpectedNodeType};
use alloc::borrow::Cow;
use alloc::collections::VecDeque;
use cid::Cid;
use core::convert::TryFrom;
use core::fmt;

/// A cache of data structures used while traversing. Reduces allocations when walking over multiple
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
    links: VecDeque<Cid>,
    // this will be tricky if we ever need to have case-insensitive resolving *but* we can then
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

    /// Transforms this `ShardedLookup` into a `ShardedLookup<'static>` by taking ownership of the
    /// needle we are trying to find.
    pub fn with_owned_needle(self) -> ShardedLookup<'static> {
        let ShardedLookup { links, needle } = self;
        let needle = Cow::Owned(needle.into_owned());
        ShardedLookup { links, needle }
    }

    /// Finds or starts a lookup of multiple buckets.
    ///
    /// Returns the found link, the definitive negative or the means to continue the traversal.
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
    /// Returns an error if we don't support the properties on the HAMTShard-typed node
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
                blocksizes: core::mem::take(&mut hamt.data.blocksizes),
            })
        } else {
            Ok(())
        }
    }

    /// Partition the original links based on their kind; if the link:
    ///
    ///  - matches the needle uniquely, it will be returned as `Some(cid)`
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
    UnexpectedBucketType(UnexpectedNodeType),
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
            UnexpectedBucketType(ut) => write!(fmt, "unexpected type for HAMT bucket: {:?}", ut),
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
    use super::{LookupError, MaybeResolved, ShardError, ShardedLookup};
    use crate::pb::FlatUnixFs;
    use core::convert::TryFrom;
    use hex_literal::hex;

    // a directory from some linux kernel tree import: linux-5.5-rc5/tools/testing/selftests/rcutorture/
    const DIR: &[u8] = &hex!("122e0a2212204baf5104fe53d495223f8e2ba95375a31fda6b18e926cb54edd61f30b5f1de6512053641646f6318b535122c0a221220fd9f545068048e647d5d0b275ed171596e0c1c04b8fed09dc13bee7607e75bc7120242391883c00312330a2212208a4a68f6b88594ce373419586c12d24bde2d519ab636b1d2dcc986eb6265b7a3120a43444d616b6566696c65189601122f0a2212201ededc99d23a7ef43a8f17e6dd8b89934993245ef39e18936a37e412e536ed681205463562696e18c5ad030a280805121f200000000020000200000000000000000004000000000000000000000000002822308002");
    const FILE: &[u8] = &hex!("0a130802120d666f6f6261720a666f6f626172180d");

    #[test]
    fn direct_hit() {
        let parsed = FlatUnixFs::try_from(DIR).unwrap();

        // calling shardedlookup directly makes little sense, but through `resolve` it would make
        // sense

        // testing this is a bit ... not nice, since we can only find out if there is a negative
        // hit through exhausting the buckets
        let found = ShardedLookup::lookup_or_start(parsed, "bin", &mut None);

        match found {
            Ok(MaybeResolved::Found(cid))
                if cid.to_string() == "QmQRA3JX9JNSccQpXjuKzMVCpfTaP4XpHbrrefqaQFWf5Z" => {}
            x => unreachable!("{:?}", x),
        }
    }

    #[test]
    fn found_in_the_other_bucket() {
        let parsed = FlatUnixFs::try_from(DIR).unwrap();

        // there is a single bin "B9" which would contain "formal" but our implementation cannot
        // see it, it just guesses to start looking up in other buckets
        let see_next = ShardedLookup::lookup_or_start(parsed, "formal", &mut None);

        let next = match see_next {
            Ok(MaybeResolved::NeedToLoadMore(next)) => next,
            x => unreachable!("{:?}", x),
        };

        {
            let (first, mut rest) = next.pending_links();

            // there is only one bin: in other cases we would just walk in BFS order
            assert_eq!(
                first.to_string(),
                "QmfQgmYMYmGQP4X6V3JhTELkQmGVP9kpJgv9duejQ8vWez"
            );
            assert!(rest.next().is_none());
        }

        // then we error on anything other than HAMTShard
        let err = next.continue_walk(FILE, &mut None).unwrap_err();
        match err {
            LookupError::UnexpectedBucketType(ut) if ut.is_file() => {}
            x => unreachable!("{:?}", x),
        }
    }

    #[test]
    fn unsupported_hash_type_or_fanout() {
        use crate::pb::{FlatUnixFs, UnixFs, UnixFsType};
        use alloc::borrow::Cow;

        let example = FlatUnixFs {
            data: UnixFs {
                Type: UnixFsType::HAMTShard,
                Data: Some(Cow::Borrowed(
                    b"this cannot be interpreted yet but would be an error",
                )),
                filesize: None,
                blocksizes: Vec::new(),
                hashType: Some(33), // supported 34 or murmur128 le cut as u64?
                fanout: Some(255),  // supported 256
                mode: None,         // these are not read by the lookup
                mtime: None,
            },
            links: Vec::new(),
        };

        let err = ShardedLookup::lookup_or_start(example, "doesnt matter", &mut None).unwrap_err();
        assert!(
            matches!(
                err,
                LookupError::Shard(ShardError::UnsupportedProperties { .. })
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn unexpected_properties() {
        use crate::pb::{FlatUnixFs, UnixFs, UnixFsType};
        use alloc::borrow::Cow;

        let example = FlatUnixFs {
            data: UnixFs {
                Type: UnixFsType::HAMTShard,
                Data: Some(Cow::Borrowed(
                    b"this cannot be interpreted yet but would be an error",
                )),
                filesize: Some(1),   // err
                blocksizes: vec![1], // err
                hashType: Some(34),
                fanout: Some(256),
                mode: None,
                mtime: None,
            },
            links: Vec::new(),
        };

        let err = ShardedLookup::lookup_or_start(example, "doesnt matter", &mut None).unwrap_err();
        assert!(
            matches!(
                err,
                LookupError::Shard(ShardError::UnexpectedProperties { .. })
            ),
            "{:?}",
            err
        );
    }
}
