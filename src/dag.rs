use crate::error::Error;
use crate::ipld::{decode_ipld, encode_ipld, Ipld};
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use crate::Ipfs;
use bitswap::Block;
use cid::{Cid, Codec, Version};

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
        let cid = match path.root().cid() {
            Some(cid) => cid,
            None => return Err(anyhow::anyhow!("expected cid")),
        };
        let mut ipld = decode_ipld(&cid, self.ipfs.repo.get_block(&cid).await?.data())?;
        for sub_path in path.iter() {
            ipld = match try_resolve(ipld, sub_path)? {
                Ipld::Link(cid) => decode_ipld(&cid, self.ipfs.repo.get_block(&cid).await?.data())?,
                ipld => ipld,
            };
        }
        Ok(ipld)
    }
}

fn try_resolve(ipld: Ipld, segment: &str) -> Result<Ipld, Error> {
    match ipld {
        Ipld::Map(mut map) => map
            .remove(segment)
            .ok_or_else(|| anyhow::anyhow!("no such segment: {:?}", segment)),
        Ipld::List(mut vec) => match segment.parse::<usize>() {
            Ok(index) if index < vec.len() => Ok(vec.swap_remove(index)),
            Ok(_) => Err(anyhow::anyhow!(
                "index out of range 0..{}: {:?}",
                vec.len(),
                segment
            )),
            Err(_) => Err(anyhow::anyhow!("invalid list index: {:?}", segment)),
        },
        link @ Ipld::Link(_) if segment == "." => Ok(link),
        other => Err(anyhow::anyhow!(
            "cannot resolve {:?} through: {:?}",
            segment,
            other
        )),
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
}
