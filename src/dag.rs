//! `ipfs.dag` interface implementation around [`Ipfs`].

use crate::error::Error;
use crate::ipld::{decode_ipld, encode_ipld, Ipld};
use crate::path::{IpfsPath, SlashedPath};
use crate::repo::RepoTypes;
use crate::{Block, Ipfs};
use cid::{Cid, Codec, Version};
use ipfs_unixfs::{
    dagpb::{wrap_node_data, NodeData},
    dir::{Cache, ShardedLookup},
    resolve, MaybeResolved,
};
use std::convert::TryFrom;
use std::error::Error as StdError;
use std::iter::Peekable;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResolveError {
    /// Loading of the block on the path failed
    #[error("block loading failed")]
    Loading(Cid, #[source] crate::Error),

    /// The document is unsupported; this can be a UnixFs directory structure which has unsupported
    /// options, or IPLD parsing failed.
    #[error("unsupported document")]
    UnsupportedDocument(Cid, #[source] Box<dyn StdError + Send + Sync + 'static>),

    /// Path contained an index which was out of range for the given [`Ipld::List`].
    #[error("list index out of range 0..{elements}: {index}")]
    ListIndexOutOfRange {
        /// The document with the mismatched index
        document: Cid,
        /// The path up until the mismatched index
        path: SlashedPath,
        /// The index in original path
        index: usize,
        /// Total number of elements found
        elements: usize,
    },

    /// Path attempted to resolve through e.g. a string or an integer.
    #[error("tried to resolve through an object that had no links")]
    NoLinks(Cid, SlashedPath),

    /// Path attempted to resolve through a property, index or link which did not exist.
    #[error("no link named {:?} under {0}", .1.iter().last().unwrap())]
    NotFound(Cid, SlashedPath),

    /// Tried to use a path neiter containing nor resolving to a Cid.
    #[error("the path neiter contains nor resolves to a Cid")]
    NoCid(IpfsPath),

    /// Couldn't resolve a path via IPNS.
    #[error("can't resolve an IPNS path")]
    IpnsResolutionFailed(IpfsPath),
}

#[derive(Debug, Error)]
pub enum UnexpectedResolved {
    #[error("path resolved to unexpected type of document: {:?} or {}", .0, .1.source())]
    UnexpectedCodec(cid::Codec, ResolvedNode),
    #[error("path did not resolve to a block on {}", .0.source())]
    NonBlock(ResolvedNode),
}

/// Used internally before translating to ResolveError at the top level by using the IpfsPath.
#[derive(Debug)]
enum RawResolveLocalError {
    Loading(Cid, crate::Error),
    UnsupportedDocument(Cid, Box<dyn StdError + Send + Sync + 'static>),
    ListIndexOutOfRange {
        document: Cid,
        segment_index: usize,
        index: usize,
        elements: usize,
    },
    InvalidIndex {
        document: Cid,
        segment_index: usize,
    },
    NoLinks {
        document: Cid,
        segment_index: usize,
    },
    NotFound {
        document: Cid,
        segment_index: usize,
    },
}

impl RawResolveLocalError {
    /// When resolving through multiple documents the local resolving functions `resolve_local_ipld`
    /// and `resolve_local_dagpb` return local document indices; need to bump the indices with the
    /// number of the already matched segments in the previous documents for the path.
    fn add_starting_point_in_path(&mut self, start: usize) {
        use RawResolveLocalError::*;
        match self {
            ListIndexOutOfRange {
                ref mut segment_index,
                ..
            }
            | InvalidIndex {
                ref mut segment_index,
                ..
            }
            | NoLinks {
                ref mut segment_index,
                ..
            }
            | NotFound {
                ref mut segment_index,
                ..
            } => {
                // NOTE: this is the **index** compared to the number of segments matched, i.e. **count**
                // from `resolve_local`'s Ok return value.
                *segment_index += start;
            }
            _ => {}
        }
    }

    /// Use the given [`IpfsPath`] to create the truncated [`SlashedPath`] and convert into
    /// [`ResolveError`]. The path is truncated so that the last segment is the one which failed to
    /// match. No reason it couldn't also be signified with just an index.
    fn with_path(self, path: IpfsPath) -> ResolveError {
        use RawResolveLocalError::*;

        match self {
            // FIXME: I'd like to use Result<Result<_, ResolveError>, crate::Error> instead
            Loading(cid, e) => ResolveError::Loading(cid, e),
            UnsupportedDocument(cid, e) => ResolveError::UnsupportedDocument(cid, e),
            ListIndexOutOfRange {
                document,
                segment_index,
                index,
                elements,
            } => ResolveError::ListIndexOutOfRange {
                document,
                path: path.into_truncated(segment_index + 1),
                index,
                elements,
            },
            NoLinks {
                document,
                segment_index,
            } => ResolveError::NoLinks(document, path.into_truncated(segment_index + 1)),
            InvalidIndex {
                document,
                segment_index,
            }
            | NotFound {
                document,
                segment_index,
            } => ResolveError::NotFound(document, path.into_truncated(segment_index + 1)),
        }
    }
}

/// `ipfs.dag` interface providing wrapper around Ipfs.
#[derive(Clone, Debug)]
pub struct IpldDag<Types: RepoTypes> {
    ipfs: Ipfs<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    /// Creates a new `IpldDag` for DAG operations.
    // FIXME: duplicates Ipfs::dag(), having both is redundant.
    pub fn new(ipfs: Ipfs<Types>) -> Self {
        IpldDag { ipfs }
    }

    /// Returns the `Cid` of a newly inserted block.
    ///
    /// The block is created from the `data`, encoded with the `codec` and inserted into the repo.
    pub async fn put(&self, data: Ipld, codec: Codec) -> Result<Cid, Error> {
        let bytes = encode_ipld(&data, codec)?;
        let hash = multihash::Sha2_256::digest(&bytes);
        let version = if codec == Codec::DagProtobuf {
            Version::V0
        } else {
            Version::V1
        };
        let cid = Cid::new(version, codec, hash)?;
        let block = Block::new(bytes, cid);
        let (cid, _) = self.ipfs.repo.put_block(block).await?;
        Ok(cid)
    }

    /// Resolves a `Cid`-rooted path to a document "node."
    ///
    /// Returns the resolved node as `Ipld`.
    pub async fn get(&self, path: IpfsPath) -> Result<Ipld, ResolveError> {
        let resolved_path = self
            .ipfs
            .resolve_ipns(&path, true)
            .await
            .map_err(|_| ResolveError::IpnsResolutionFailed(path))?;

        let cid = match resolved_path.root().cid() {
            Some(cid) => cid,
            None => return Err(ResolveError::NoCid(resolved_path)),
        };

        let mut iter = resolved_path.iter().peekable();

        let (node, _) = match self.resolve0(cid, &mut iter, true).await {
            Ok(t) => t,
            Err(e) => {
                drop(iter);
                return Err(e.with_path(resolved_path));
            }
        };

        Ipld::try_from(node)
    }

    /// Resolves a `Cid`-rooted path to a document "node."
    ///
    /// The return value has two kinds of meanings depending on whether links should be followed or
    /// not: when following links, the second returned value will be the path inside the last document;
    /// when not following links, the second returned value will be the unmatched or "remaining"
    /// path.
    ///
    /// Regardless of the `follow_links` option, HAMT-sharded directories will be resolved through
    /// as a "single step" in the given IpfsPath.
    ///
    /// Returns a node and the remaining path or the path inside the last document.
    pub async fn resolve(
        &self,
        path: IpfsPath,
        follow_links: bool,
    ) -> Result<(ResolvedNode, SlashedPath), ResolveError> {
        let resolved_path = self
            .ipfs
            .resolve_ipns(&path, true)
            .await
            .map_err(|_| ResolveError::IpnsResolutionFailed(path))?;

        let cid = match resolved_path.root().cid() {
            Some(cid) => cid,
            None => return Err(ResolveError::NoCid(resolved_path)),
        };

        let (node, matched_segments) = {
            let mut iter = resolved_path.iter().peekable();
            match self.resolve0(cid, &mut iter, follow_links).await {
                Ok(t) => t,
                Err(e) => {
                    drop(iter);
                    return Err(e.with_path(resolved_path));
                }
            }
        };

        // we only care about returning this remaining_path with segments up until the last
        // document but it can and should contain all of the following segments (if any). there
        // could be more segments when `!follow_links`.
        let remaining_path = resolved_path.into_shifted(matched_segments);

        Ok((node, remaining_path))
    }

    /// Return the node where the resolving ended, and the **count** of segments matched.
    async fn resolve0<'a>(
        &self,
        cid: &Cid,
        segments: &mut Peekable<impl Iterator<Item = &'a str>>,
        follow_links: bool,
    ) -> Result<(ResolvedNode, usize), RawResolveLocalError> {
        use LocallyResolved::*;

        let mut current = cid.to_owned();
        let mut total = 0;

        let mut cache = None;

        loop {
            let block = match self.ipfs.repo.get_block(&current).await {
                Ok(block) => block,
                Err(e) => return Err(RawResolveLocalError::Loading(current, e)),
            };

            let start = total;

            let (resolution, matched) = match resolve_local(block, segments, &mut cache) {
                Ok(t) => t,
                Err(mut e) => {
                    e.add_starting_point_in_path(start);
                    return Err(e);
                }
            };
            total += matched;

            let (src, dest) = match resolution {
                Complete(ResolvedNode::Link(src, dest)) => (src, dest),
                Incomplete(src, lookup) => match self.resolve_hamt(lookup, &mut cache).await {
                    Ok(dest) => (src, dest),
                    Err(e) => return Err(RawResolveLocalError::UnsupportedDocument(src, e.into())),
                },
                Complete(other) => {
                    // when following links we return the total of links matched before the
                    // returned document.
                    return Ok((other, start));
                }
            };

            if !follow_links {
                // when not following links we return the total of links matched
                return Ok((ResolvedNode::Link(src, dest), total));
            } else {
                current = dest;
            }
        }
    }

    /// To resolve a segment through a HAMT-sharded directory we need to load more blocks, which is
    /// why this is a method and not a free `fn` like the other resolving activities.
    async fn resolve_hamt(
        &self,
        mut lookup: ShardedLookup<'_>,
        cache: &mut Option<Cache>,
    ) -> Result<Cid, Error> {
        use MaybeResolved::*;

        loop {
            let (next, _) = lookup.pending_links();

            let block = self.ipfs.repo.get_block(next).await?;

            match lookup.continue_walk(block.data(), cache)? {
                NeedToLoadMore(next) => lookup = next,
                Found(cid) => return Ok(cid),
                NotFound => return Err(anyhow::anyhow!("key not found: ???")),
            }
        }
    }
}

/// `IpfsPath`'s `Cid`-based variant can be resolved to the block, projections represented by this
/// type.
///
/// Values can be converted to Ipld using `Ipld::try_from`.
#[derive(Debug, PartialEq)]
pub enum ResolvedNode {
    /// Block which was loaded at the end of the path.
    Block(Block),
    /// Path ended in `Data` at a dag-pb node. This is usually not interesting and should be
    /// treated as a "Not found" error since dag-pb node did not have a *link* called `Data`. The variant
    /// exists as there are interface-ipfs-http tests which require this behaviour.
    DagPbData(Cid, NodeData<Box<[u8]>>),
    /// Path ended on a !dag-pb document which was projected.
    Projection(Cid, Ipld),
    /// Local resolving ended with a link
    Link(Cid, Cid),
}

impl ResolvedNode {
    /// Returns the `Cid` of the **source** document for the encapsulated document or projection of such.
    pub fn source(&self) -> &Cid {
        match self {
            ResolvedNode::Block(Block { cid, .. })
            | ResolvedNode::DagPbData(cid, ..)
            | ResolvedNode::Projection(cid, ..)
            | ResolvedNode::Link(cid, ..) => cid,
        }
    }

    /// Unwraps the dagpb block variant and turns others into UnexpectedResolved.
    /// This is useful wherever unixfs operations are continued after resolving an IpfsPath.
    pub fn into_unixfs_block(self) -> Result<Block, UnexpectedResolved> {
        if self.source().codec() != cid::Codec::DagProtobuf {
            Err(UnexpectedResolved::UnexpectedCodec(
                cid::Codec::DagProtobuf,
                self,
            ))
        } else {
            match self {
                ResolvedNode::Block(b) => Ok(b),
                _ => Err(UnexpectedResolved::NonBlock(self)),
            }
        }
    }
}

impl TryFrom<ResolvedNode> for Ipld {
    type Error = ResolveError;
    fn try_from(r: ResolvedNode) -> Result<Ipld, Self::Error> {
        use ResolvedNode::*;

        match r {
            Block(block) => Ok(decode_ipld(block.cid(), block.data())
                .map_err(move |e| ResolveError::UnsupportedDocument(block.cid, e.into()))?),
            DagPbData(_, node_data) => Ok(Ipld::Bytes(node_data.node_data().to_vec())),
            Projection(_, ipld) => Ok(ipld),
            Link(_, cid) => Ok(Ipld::Link(cid)),
        }
    }
}

/// Success variants for the `resolve_local` operation on an `Ipld` document.
#[derive(Debug)]
enum LocallyResolved<'a> {
    /// Resolution completed.
    Complete(ResolvedNode),

    /// Resolving was attempted on a block which is a HAMT-sharded bucket, and needs to be
    /// continued by loading other buckets.
    Incomplete(Cid, ShardedLookup<'a>),
}

#[cfg(test)]
impl LocallyResolved<'_> {
    fn unwrap_complete(self) -> ResolvedNode {
        match self {
            LocallyResolved::Complete(rn) => rn,
            x => unreachable!("{:?}", x),
        }
    }
}

impl From<ResolvedNode> for LocallyResolved<'static> {
    fn from(r: ResolvedNode) -> LocallyResolved<'static> {
        LocallyResolved::Complete(r)
    }
}

/// Resolves the given path segments locally or inside the given document; in addition to
/// `resolve_local_ipld` this fn also handles normal dag-pb and unixfs HAMTs.
fn resolve_local<'a>(
    block: Block,
    segments: &mut Peekable<impl Iterator<Item = &'a str>>,
    cache: &mut Option<Cache>,
) -> Result<(LocallyResolved<'a>, usize), RawResolveLocalError> {
    if segments.peek().is_none() {
        return Ok((LocallyResolved::Complete(ResolvedNode::Block(block)), 0));
    }

    let Block { cid, data } = block;

    if cid.codec() == cid::Codec::DagProtobuf {
        // special-case the dagpb since we need to do the HAMT lookup and going through the
        // BTreeMaps of ipld for this is quite tiresome. if you are looking for that code for
        // simple directories, you can find one in the history of ipfs-http.

        // advancing is required here in order for us to determine if this was the last element.
        // This should be ok as the only way we can continue resolving deeper is the case of Link
        // being matched, and not the error or the DagPbData case.
        let segment = segments.next().unwrap();

        Ok(resolve_local_dagpb(
            cid,
            data,
            segment,
            segments.peek().is_none(),
            cache,
        )?)
    } else {
        let ipld = match decode_ipld(&cid, &data) {
            Ok(ipld) => ipld,
            Err(e) => return Err(RawResolveLocalError::UnsupportedDocument(cid, e.into())),
        };
        resolve_local_ipld(cid, ipld, segments)
    }
}

/// Resolving through dagpb documents is basically just mapping from [`MaybeResolved`] to the
/// return value, with the exception that a path ending in "Data" is returned as
/// `ResolvedNode::DagPbData`.
fn resolve_local_dagpb<'a>(
    cid: Cid,
    data: Box<[u8]>,
    segment: &'a str,
    is_last: bool,
    cache: &mut Option<Cache>,
) -> Result<(LocallyResolved<'a>, usize), RawResolveLocalError> {
    match resolve(&data, segment, cache) {
        Ok(MaybeResolved::NeedToLoadMore(lookup)) => {
            Ok((LocallyResolved::Incomplete(cid, lookup), 0))
        }
        Ok(MaybeResolved::Found(dest)) => {
            Ok((LocallyResolved::Complete(ResolvedNode::Link(cid, dest)), 1))
        }
        Ok(MaybeResolved::NotFound) => {
            if segment == "Data" && is_last {
                let wrapped = wrap_node_data(data).expect("already deserialized once");
                return Ok((
                    LocallyResolved::Complete(ResolvedNode::DagPbData(cid, wrapped)),
                    1,
                ));
            }
            Err(RawResolveLocalError::NotFound {
                document: cid,
                segment_index: 0,
            })
        }
        Err(ipfs_unixfs::ResolveError::UnexpectedType(ut)) if ut.is_file() => {
            // this might even be correct: files we know are not supported, however not sure if
            // symlinks are, let alone custom unxifs types should such exist
            Err(RawResolveLocalError::NotFound {
                document: cid,
                segment_index: 0,
            })
        }
        Err(e) => Err(RawResolveLocalError::UnsupportedDocument(cid, e.into())),
    }
}

/// Resolves the given path segments locally or inside the given document. Resolving is terminated
/// upon reaching a link or exhausting the path.
///
/// Returns the number of path segments matched -- the iterator might be consumed more than it was
/// matched.
///
/// Note: Tried to initially work with this through Peekable but this would need two peeks.
///
/// # Limitations
///
/// Does not support dag-pb as segments are resolved differently on dag-pb than the general Ipld.
fn resolve_local_ipld<'a>(
    document: Cid,
    mut ipld: Ipld,
    segments: &mut Peekable<impl Iterator<Item = &'a str>>,
) -> Result<(LocallyResolved<'a>, usize), RawResolveLocalError> {
    let mut matched_count = 0;
    loop {
        ipld = match ipld {
            Ipld::Link(cid) => {
                if segments.peek() != Some(&".") {
                    // there is something other than dot next in the path, we should silently match
                    // over it.
                    return Ok((ResolvedNode::Link(document, cid).into(), matched_count));
                } else {
                    Ipld::Link(cid)
                }
            }
            ipld => ipld,
        };

        ipld = match (ipld, segments.next()) {
            (Ipld::Link(cid), Some(".")) => {
                return Ok((ResolvedNode::Link(document, cid).into(), matched_count + 1));
            }
            (Ipld::Link(_), Some(_)) => {
                unreachable!("case already handled above before advancing the iterator")
            }
            (Ipld::Map(mut map), Some(segment)) => {
                let found = match map.remove(segment) {
                    Some(f) => f,
                    None => {
                        return Err(RawResolveLocalError::NotFound {
                            document,
                            segment_index: matched_count,
                        })
                    }
                };
                matched_count += 1;
                found
            }
            (Ipld::List(mut vec), Some(segment)) => match segment.parse::<usize>() {
                Ok(index) if index < vec.len() => {
                    matched_count += 1;
                    vec.swap_remove(index)
                }
                Ok(index) => {
                    return Err(RawResolveLocalError::ListIndexOutOfRange {
                        document,
                        segment_index: matched_count,
                        index,
                        elements: vec.len(),
                    });
                }
                Err(_) => {
                    return Err(RawResolveLocalError::InvalidIndex {
                        document,
                        segment_index: matched_count,
                    })
                }
            },
            (_, Some(_)) => {
                return Err(RawResolveLocalError::NoLinks {
                    document,
                    segment_index: matched_count,
                });
            }
            // path has been consumed
            (anything, None) => {
                return Ok((
                    ResolvedNode::Projection(document, anything).into(),
                    matched_count,
                ))
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{make_ipld, Node};

    #[tokio::test]
    async fn test_resolve_root_cid() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, 2, 3]);
        let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
        let res = dag.get(IpfsPath::from(cid)).await.unwrap();
        assert_eq!(res, data);
    }

    #[tokio::test]
    async fn test_resolve_array_elem() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, 2, 3]);
        let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid).sub_path("1").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(2));
    }

    #[tokio::test]
    async fn test_resolve_nested_array_elem() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, [2], 3,]);
        let cid = dag.put(data, Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid).sub_path("1/0").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(2));
    }

    #[tokio::test]
    async fn test_resolve_object_elem() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!({
            "key": false,
        });
        let cid = dag.put(data, Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid).sub_path("key").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(false));
    }

    #[tokio::test]
    async fn test_resolve_cid_elem() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data1 = make_ipld!([1]);
        let cid1 = dag.put(data1, Codec::DagCBOR).await.unwrap();
        let data2 = make_ipld!([cid1]);
        let cid2 = dag.put(data2, Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid2).sub_path("0/0").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(1));
    }

    /// Returns an example ipld document with strings, ints, maps, lists, and a link. The link target is also
    /// returned.
    fn example_doc_and_cid() -> (Cid, Ipld, Cid) {
        let cid = Cid::try_from("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let doc = make_ipld!({
            "nested": {
                "even": [
                    {
                        "more": 5
                    },
                    {
                        "or": "this",
                    },
                    {
                        "or": cid.clone(),
                    },
                    {
                        "5": "or",
                    }
                ],
            }
        });
        let root =
            Cid::try_from("bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita").unwrap();
        (root, doc, cid)
    }

    #[test]
    fn resolve_cbor_locally_to_end() {
        let (root, example_doc, _) = example_doc_and_cid();

        let good_examples = [
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/0/more",
                Ipld::Integer(5),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/1/or",
                Ipld::from("this"),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/3/5",
                Ipld::from("or"),
            ),
        ];

        for (path, expected) in &good_examples {
            let p = IpfsPath::try_from(*path).unwrap();

            let (resolved, matched_segments) = super::resolve_local_ipld(
                root.clone(),
                example_doc.clone(),
                &mut p.iter().peekable(),
            )
            .unwrap();

            assert_eq!(matched_segments, 4);

            match resolved.unwrap_complete() {
                ResolvedNode::Projection(_, p) if &p == expected => {}
                x => unreachable!("unexpected {:?}", x),
            }

            let remaining_path = p.iter().skip(matched_segments).collect::<Vec<&str>>();
            assert!(remaining_path.is_empty(), "{:?}", remaining_path);
        }
    }

    #[test]
    fn resolve_cbor_locally_to_link() {
        let (root, example_doc, target) = example_doc_and_cid();

        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/2/or/foobar/trailer"
            // counts:                                                    1      2   3 4
        ).unwrap();

        let (resolved, matched_segments) =
            super::resolve_local_ipld(root, example_doc, &mut p.iter().peekable()).unwrap();

        match resolved.unwrap_complete() {
            ResolvedNode::Link(_, cid) if cid == target => {}
            x => unreachable!("{:?}", x),
        }

        assert_eq!(matched_segments, 4);

        let remaining_path = p.iter().skip(matched_segments).collect::<Vec<&str>>();
        assert_eq!(remaining_path, &["foobar", "trailer"]);
    }

    #[test]
    fn resolve_cbor_locally_to_link_with_dot() {
        let (root, example_doc, cid) = example_doc_and_cid();

        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/2/or/./foobar/trailer",
            // counts:                                                    1      2   3 4  5
        )
        .unwrap();

        let (resolved, matched_segments) =
            super::resolve_local_ipld(root.clone(), example_doc, &mut p.iter().peekable()).unwrap();
        assert_eq!(resolved.unwrap_complete(), ResolvedNode::Link(root, cid));
        assert_eq!(matched_segments, 5);

        let remaining_path = p.iter().skip(matched_segments).collect::<Vec<&str>>();
        assert_eq!(remaining_path, &["foobar", "trailer"]);
    }

    #[test]
    fn resolve_cbor_locally_not_found_map_key() {
        let (root, example_doc, _) = example_doc_and_cid();
        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/foobar/trailer",
        )
        .unwrap();

        let e = super::resolve_local_ipld(root, example_doc, &mut p.iter().peekable()).unwrap_err();
        assert!(
            matches!(
                e,
                RawResolveLocalError::NotFound {
                    segment_index: 0,
                    ..
                }
            ),
            "{:?}",
            e
        );
    }

    #[test]
    fn resolve_cbor_locally_too_large_list_index() {
        let (root, example_doc, _) = example_doc_and_cid();
        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/3000",
        )
        .unwrap();

        let e = super::resolve_local_ipld(root, example_doc, &mut p.iter().peekable()).unwrap_err();
        assert!(
            matches!(
                e,
                RawResolveLocalError::ListIndexOutOfRange {
                    segment_index: 2,
                    index: 3000,
                    elements: 4,
                    ..
                }
            ),
            "{:?}",
            e
        );
    }

    #[test]
    fn resolve_cbor_locally_non_usize_index() {
        let (root, example_doc, _) = example_doc_and_cid();
        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/-1",
        )
        .unwrap();

        // FIXME: errors, again the number of matched
        let e = super::resolve_local_ipld(root, example_doc, &mut p.iter().peekable()).unwrap_err();
        assert!(
            matches!(
                e,
                RawResolveLocalError::InvalidIndex {
                    segment_index: 2,
                    ..
                }
            ),
            "{:?}",
            e
        );
    }

    #[tokio::test]
    async fn resolve_through_link() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let ipld = make_ipld!([1]);
        let cid1 = dag.put(ipld, Codec::DagCBOR).await.unwrap();
        let ipld = make_ipld!([cid1]);
        let cid2 = dag.put(ipld, Codec::DagCBOR).await.unwrap();

        let prefix = IpfsPath::from(cid2);

        // the two should be equal, as dot can appear or not appear
        // FIXME: validate that go-ipfs still does this
        let equiv_paths = vec![
            prefix.sub_path("0/0").unwrap(),
            prefix.sub_path("0/./0").unwrap(),
        ];

        for p in equiv_paths {
            let cloned = p.clone();
            match dag.resolve(p, true).await.unwrap() {
                (ResolvedNode::Projection(_, Ipld::Integer(1)), remaining_path) => {
                    assert_eq!(remaining_path, ["0"][..], "{}", cloned);
                }
                x => unreachable!("{:?}", x),
            }
        }
    }

    #[tokio::test]
    async fn fail_resolving_first_segment() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let ipld = make_ipld!([1]);
        let cid1 = dag.put(ipld, Codec::DagCBOR).await.unwrap();
        let ipld = make_ipld!({ "0": cid1 });
        let cid2 = dag.put(ipld, Codec::DagCBOR).await.unwrap();

        let path = IpfsPath::from(cid2.clone()).sub_path("1/a").unwrap();

        //let cloned = path.clone();
        let e = dag.resolve(path, true).await.unwrap_err();
        assert_eq!(e.to_string(), format!("no link named \"1\" under {}", cid2));
    }

    #[tokio::test]
    async fn fail_resolving_last_segment() {
        let Node { ipfs, .. } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let ipld = make_ipld!([1]);
        let cid1 = dag.put(ipld, Codec::DagCBOR).await.unwrap();
        let ipld = make_ipld!([cid1.clone()]);
        let cid2 = dag.put(ipld, Codec::DagCBOR).await.unwrap();

        let path = IpfsPath::from(cid2).sub_path("0/a").unwrap();

        //let cloned = path.clone();
        let e = dag.resolve(path, true).await.unwrap_err();
        assert_eq!(e.to_string(), format!("no link named \"a\" under {}", cid1));
    }

    #[tokio::test]
    async fn fail_resolving_through_file() {
        let Node { ipfs, .. } = Node::new("test_node").await;

        let mut adder = ipfs_unixfs::file::adder::FileAdder::default();
        let (mut blocks, _) = adder.push(b"foobar\n");
        assert_eq!(blocks.next(), None);

        let mut blocks = adder.finish();

        let (cid, data) = blocks.next().unwrap();
        assert_eq!(blocks.next(), None);

        ipfs.put_block(Block {
            cid: cid.clone(),
            data: data.into(),
        })
        .await
        .unwrap();

        let path = IpfsPath::from(cid.clone())
            .sub_path("anything-here")
            .unwrap();

        let e = ipfs.dag().resolve(path, true).await.unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("no link named \"anything-here\" under {}", cid)
        );
    }

    #[tokio::test]
    async fn fail_resolving_through_dir() {
        let Node { ipfs, .. } = Node::new("test_node").await;

        let mut adder = ipfs_unixfs::file::adder::FileAdder::default();
        let (mut blocks, _) = adder.push(b"foobar\n");
        assert_eq!(blocks.next(), None);

        let mut blocks = adder.finish();

        let (cid, data) = blocks.next().unwrap();
        assert_eq!(blocks.next(), None);

        let total_size = data.len();

        ipfs.put_block(Block {
            cid: cid.clone(),
            data: data.into(),
        })
        .await
        .unwrap();

        let mut opts = ipfs_unixfs::dir::builder::TreeOptions::default();
        opts.wrap_with_directory();

        let mut tree = ipfs_unixfs::dir::builder::BufferingTreeBuilder::new(opts);
        tree.put_link("something/best-file-in-the-world", cid, total_size as u64)
            .unwrap();

        let mut iter = tree.build();
        let mut cids = Vec::new();

        while let Some(node) = iter.next_borrowed() {
            let node = node.unwrap();
            let block = Block {
                cid: node.cid.to_owned(),
                data: node.block.into(),
            };

            ipfs.put_block(block).await.unwrap();

            cids.push(node.cid.to_owned());
        }

        // reverse the cids because they now contain the root cid as the last.
        cids.reverse();

        let path = IpfsPath::from(cids[0].to_owned())
            .sub_path("something/second-best-file")
            .unwrap();

        let e = ipfs.dag().resolve(path, true).await.unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("no link named \"second-best-file\" under {}", cids[1])
        );
    }
}
