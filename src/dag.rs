use crate::error::Error;
use crate::ipld::{decode_ipld, encode_ipld, Ipld};
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use crate::Ipfs;
use bitswap::Block;
use cid::{Cid, Codec, Version};
use ipfs_unixfs::{dagpb::NodeData, dir::ShardedLookup, resolve, MaybeResolved, ResolveError};
use std::convert::TryFrom;
use std::fmt;
use std::iter::Peekable;

#[derive(Clone, Debug)]
pub struct IpldDag<Types: RepoTypes> {
    ipfs: Ipfs<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    pub fn new(ipfs: Ipfs<Types>) -> Self {
        IpldDag { ipfs }
    }

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

    pub async fn get(&self, path: IpfsPath) -> Result<Ipld, Error> {
        // FIXME: do ipns resolve first
        let cid = match path.root().cid() {
            Some(cid) => cid,
            None => return Err(anyhow::anyhow!("expected cid")),
        };

        let mut iter = path.iter().peekable();

        let (node, _) = self.resolve0(cid, &mut iter, true).await?;

        Ipld::try_from(node)
    }

    pub async fn resolve(
        &self,
        path: IpfsPath,
        follow_links: bool,
    ) -> Result<(ResolvedNode, SlashedPath), Error> {
        // FIXME: do ipns resolve first
        let cid = match path.root().cid() {
            Some(cid) => cid,
            None => return Err(anyhow::anyhow!("expected cid")),
        };

        let mut iter = path.iter().peekable();

        let (node, matched) = self.resolve0(cid, &mut iter, follow_links).await?;

        let mut remaining_path = SlashedPath::default();
        remaining_path
            .push_split(path.iter().skip(path.len() - matched))
            .expect("these were already valid segments");

        Ok((node, remaining_path))
    }

    async fn resolve0<'a>(
        &self,
        cid: &Cid,
        segments: &mut Peekable<impl Iterator<Item = &'a str>>,
        follow_links: bool,
    ) -> Result<(ResolvedNode, usize), Error> {
        use LocallyResolved::*;

        let mut current = cid.to_owned();

        loop {
            let block = self.ipfs.repo.get_block(&current).await?;

            println!("resolving on {}: {:?}", block.cid(), segments.peek());

            let ((src, dest), matched) = match resolve_local(block, segments)? {
                (Complete(ResolvedNode::Link(src, dest)), matched) => ((src, dest), dbg!(matched)),
                (Complete(other), matched) => return Ok((other, dbg!(matched))),
                (Incomplete(src, lookup), matched) => {
                    assert_eq!(matched, 0);
                    ((src, self.resolve_hamt(lookup).await?), 1)
                }
            };

            if !follow_links {
                return Ok((ResolvedNode::Link(src, dest), matched));
            } else {
                println!("following link {}", dest);
                current = dest;
            }
        }
    }

    async fn resolve_hamt<'a>(&self, mut lookup: ShardedLookup<'a>) -> Result<Cid, Error> {
        use MaybeResolved::*;

        loop {
            let (next, _) = lookup.pending_links();

            let block = self.ipfs.repo.get_block(next).await?;

            // TODO: cache
            match lookup.continue_walk(block.data(), &mut None)? {
                NeedToLoadMore(next) => lookup = next,
                Found(cid) => return Ok(cid),
                NotFound => return Err(anyhow::anyhow!("key not found: ???")),
            }
        }
    }
}

use crate::path::SlashedPath;

// Return type of `IpfsDag::resolve`, which can be turned into familiar `/api/v0/dag/resolve`
// return value of `{ cid, remaining_path }`, or consumed as the result of `IpfsDag::get`.
//#[derive(Debug, PartialEq)]
//pub struct Resolution(ResolvedNode, SlashedPath);

/// `IpfsPath`'s cid based variant can be resolved to the three variants.
#[derive(Debug, PartialEq)]
pub enum ResolvedNode {
    /// Block which was loaded at the end of the path.
    Block(Block),
    /// Path ended in `Data` at a dag-pb node. This is usually not interesting and should be
    /// treated as "Not found" error since dag-pb node did not have a *link* called `Data`. Variant
    /// exists as there are interface-ipfs-http tests which require this behaviour.
    DagPbData(Cid, NodeData<Box<[u8]>>),
    /// Path ended on an !dag-pb document which was projected.
    Projection(Cid, Ipld),
    /// Local resolving ended in at link
    Link(Cid, Cid),
}

impl ResolvedNode {
    /// Returns the cid of the **source** document. Source or destination matters only the case of link
    /// to another document.
    pub fn source(&self) -> &Cid {
        match self {
            ResolvedNode::Block(Block { cid, .. })
            | ResolvedNode::DagPbData(cid, ..)
            | ResolvedNode::Projection(cid, ..)
            | ResolvedNode::Link(cid, ..) => cid,
        }
    }

    pub fn destination(&self) -> Option<&Cid> {
        match self {
            ResolvedNode::Link(_, cid) => Some(cid),
            _ => None,
        }
    }

    pub fn cid(&self) -> &Cid {
        self.destination().unwrap_or_else(|| self.source())
    }
}

impl TryFrom<ResolvedNode> for Ipld {
    type Error = Error;
    fn try_from(r: ResolvedNode) -> Result<Ipld, Error> {
        use ResolvedNode::*;

        match r {
            Block(block) => Ok(decode_ipld(block.cid(), block.data())?),
            DagPbData(_, node_data) => Ok(Ipld::Bytes(node_data.node_data().to_vec())),
            Projection(_, ipld) => Ok(ipld),
            Link(_, cid) => Ok(Ipld::Link(cid)),
        }
    }
}

/// Success variants for resolve_local operation on an Ipld document.
#[derive(Debug)]
enum LocallyResolved<'a> {
    /// Resolution completed
    Complete(ResolvedNode),

    /// Resolving was attempted on a block which is a HAMT sharded bucket, and needs to be
    /// continued by loading other buckets.
    Incomplete(Cid, ShardedLookup<'a>),
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
) -> Result<(LocallyResolved<'a>, usize), Error> {
    println!("resolving on {}", block.cid());
    if segments.peek().is_none() {
        return Ok((LocallyResolved::Complete(ResolvedNode::Block(block)), 0));
    }

    let Block { cid, data } = block;

    if cid.codec() == cid::Codec::DagProtobuf {
        // converting dag-pb to ipld is quite tedious, not to mention HAMT and other stuff
        // though, we cannot handle HAMT locally

        let segment = segments.next().unwrap();

        // TODO: lift the cache as parameter?
        match ipfs_unixfs::resolve(&data, segment, &mut None) {
            Ok(MaybeResolved::NeedToLoadMore(lookup)) => {
                Ok((LocallyResolved::Incomplete(cid, lookup), 0))
            }
            Ok(MaybeResolved::Found(dest)) => {
                // it's clear that we need to have the segment dropped here.
                Ok((LocallyResolved::Complete(ResolvedNode::Link(cid, dest)), 1))
            }
            Ok(MaybeResolved::NotFound) => {
                // TROUBLE: cannot honor the "pop only matched ones" anymore
                if segment == "Data" && segments.peek().is_none() {
                    let wrapped = ipfs_unixfs::dagpb::wrap_node_data(data)
                        .expect("already deserialized once");
                    return Ok((
                        LocallyResolved::Complete(ResolvedNode::DagPbData(cid, wrapped)),
                        1,
                    ));
                }
                Err(anyhow::anyhow!("no such segment: {:?}", segment))
            }
            // FIXME: need to wrap these differently for some fixed error message
            Err(e) => Err(e.into()),
        }
    } else {
        let ipld = decode_ipld(&cid, &data)?;
        let (resolved, matched) = resolve_local_ipld(ipld, segments)?;
        Ok((resolved.with_source(cid).into(), matched))
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
    mut ipld: Ipld,
    segments: &mut Peekable<impl Iterator<Item = &'a str>>,
) -> Result<(ResolvedIpld, usize), Error> {
    let mut matched = 0;
    loop {
        ipld = match ipld {
            Ipld::Link(cid) => {
                if segments.peek() != Some(&".") {
                    // there is something other than dot next in the path, we should silently match
                    // over it.
                    return Ok((ResolvedIpld::Link(cid), matched));
                } else {
                    Ipld::Link(cid)
                }
            }
            ipld => ipld,
        };

        ipld = match (ipld, segments.next()) {
            (Ipld::Link(cid), Some(".")) => {
                return Ok((ResolvedIpld::Link(cid), matched + 1));
            }
            (Ipld::Link(_), Some(_)) => {
                // TODO: verify this really matches go-ipfs behaviour
                // the idea here is to "silently" follow the link.
                unreachable!()
            }
            (Ipld::Map(mut map), Some(segment)) => {
                let found = map
                    .remove(segment)
                    .ok_or_else(|| anyhow::anyhow!("no such segment: {:?}", segment))?;
                matched += 1;
                found
            }
            (Ipld::List(mut vec), Some(segment)) => match segment.parse::<usize>() {
                Ok(index) if index < vec.len() => {
                    matched += 1;
                    vec.swap_remove(index)
                }
                Ok(_) => {
                    return Err(anyhow::anyhow!(
                        "index out of range 0..{}: {:?}",
                        vec.len(),
                        segment
                    ))
                }
                Err(_) => return Err(anyhow::anyhow!("invalid list index: {:?}", segment)),
            },
            (other, Some(segment)) => {
                return Err(anyhow::anyhow!(
                    "cannot resolve {:?} through: {:?}",
                    segment,
                    other
                ))
            }
            // path has been consumed
            (anything, None) => return Ok((ResolvedIpld::Projection(anything), matched)),
        };
    }
}

enum ResolvedIpld {
    Link(Cid),
    Projection(Ipld),
}

impl ResolvedIpld {
    fn with_source(self, source: Cid) -> ResolvedNode {
        match self {
            ResolvedIpld::Link(dest) => ResolvedNode::Link(source, dest),
            ResolvedIpld::Projection(ipld) => ResolvedNode::Projection(source, ipld),
        }
    }
}

impl fmt::Debug for ResolvedIpld {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolvedIpld::Link(cid) => write!(fmt, "Link({})", cid),
            ResolvedIpld::Projection(ipld) => write!(fmt, "Projection({:?})", ipld),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{make_ipld, Node};

    #[tokio::test(max_threads = 1)]
    async fn test_resolve_root_cid() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, 2, 3]);
        let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
        let res = dag.get(IpfsPath::from(cid)).await.unwrap();
        assert_eq!(res, data);
    }

    #[tokio::test(max_threads = 1)]
    async fn test_resolve_array_elem() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, 2, 3]);
        let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid).sub_path("1").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(2));
    }

    #[tokio::test(max_threads = 1)]
    async fn test_resolve_nested_array_elem() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
        let dag = IpldDag::new(ipfs);
        let data = make_ipld!([1, [2], 3,]);
        let cid = dag.put(data, Codec::DagCBOR).await.unwrap();
        let res = dag
            .get(IpfsPath::from(cid).sub_path("1/0").unwrap())
            .await
            .unwrap();
        assert_eq!(res, make_ipld!(2));
    }

    #[tokio::test(max_threads = 1)]
    async fn test_resolve_object_elem() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
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

    #[tokio::test(max_threads = 1)]
    async fn test_resolve_cid_elem() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
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
    fn example_doc_and_cid() -> (Ipld, Cid) {
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
        (doc, cid)
    }

    #[test]
    fn resolve_cbor_locally_to_end() {
        let (example_doc, _) = example_doc_and_cid();

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

            let (resolved, matched) =
                super::resolve_local_ipld(example_doc.clone(), &mut p.iter().peekable()).unwrap();

            match resolved {
                ResolvedIpld::Projection(p) if &p == expected => {}
                x => unreachable!("unexpected {:?}", x),
            }

            let remaining_path = p.iter().skip(matched).collect::<Vec<&str>>();
            assert!(remaining_path.is_empty(), "{:?}", remaining_path);
        }
    }

    #[test]
    fn resolve_cbor_locally_to_link() {
        let (example_doc, target) = example_doc_and_cid();
        let p = IpfsPath::try_from("bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/2/or/foobar/trailer").unwrap();

        let (resolved, matched) =
            super::resolve_local_ipld(example_doc, &mut p.iter().peekable()).unwrap();

        match resolved {
            ResolvedIpld::Link(cid) if cid == target => {}
            x => unreachable!("{:?}", x),
        }

        let remaining_path = p.iter().skip(matched).collect::<Vec<&str>>();
        assert_eq!(remaining_path, &["foobar", "trailer"]);
    }

    #[test]
    fn resolve_cbor_locally_not_found_map_key() {
        let (example_doc, _) = example_doc_and_cid();
        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/foobar/trailer",
        )
        .unwrap();

        // FIXME: errors; could give how much it was matched and so on
        super::resolve_local_ipld(example_doc, &mut p.iter().peekable()).unwrap_err();
    }

    #[test]
    fn resolve_cbor_locally_too_large_list_index() {
        let (example_doc, _) = example_doc_and_cid();
        let p = IpfsPath::try_from(
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/3000",
        )
        .unwrap();

        // FIXME: errors, again the number of matched
        super::resolve_local_ipld(example_doc, &mut p.iter().peekable()).unwrap_err();
    }

    #[tokio::test(max_threads = 1)]
    async fn resolve_through_link() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
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
            prefix.into_sub_path("0/./0").unwrap(),
        ];

        for p in equiv_paths {
            let cloned = p.clone();
            match dag.resolve(p, true).await.unwrap() {
                (ResolvedNode::Projection(_, Ipld::Integer(1)), remaining_path) => {
                    assert_eq!(remaining_path, ["0"][..], "{}", cloned);
                    println!("matched {}", cloned);
                    println!("----\n");
                }
                x => unreachable!("{:?}", x),
            }
        }
    }
}
