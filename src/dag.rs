use crate::error::Error;
use crate::path::{IpfsPath, IpfsPathError, SubPath};
use crate::repo::{Repo, RepoTypes};
use async_trait::async_trait;
use bitswap::Block;
use cid::{Cid, Codec, Version};
use libipld::block::{decode_ipld, encode_ipld};
use libipld::ipld::Ipld;

#[async_trait]
pub trait IpldDag {
    async fn put_dag(&self, data: Ipld, codec: Codec) -> Result<Cid, Error>;

    async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error>;
}

#[async_trait]
impl<T: RepoTypes> IpldDag for Repo<T> {
    async fn put_dag(&self, data: Ipld, codec: Codec) -> Result<Cid, Error> {
        let bytes = encode_ipld(&data, codec)?;
        let hash = multihash::Sha2_256::digest(&bytes);
        let version = if codec == Codec::DagProtobuf {
            Version::V0
        } else {
            Version::V1
        };
        let cid = Cid::new(version, codec, hash)?;
        let block = Block::new(bytes, cid);
        let (cid, _) = self.put_block(block).await?;
        Ok(cid)
    }

    async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        let cid = match path.root().cid() {
            Some(cid) => cid,
            None => return Err(anyhow::anyhow!("expected cid")),
        };
        let mut ipld = decode_ipld(&cid, self.get_block(&cid).await?.data())?;
        for sub_path in path.iter() {
            if !can_resolve(&ipld, sub_path) {
                let path = sub_path.to_owned();
                return Err(IpfsPathError::ResolveError { ipld, path }.into());
            }
            ipld = resolve(ipld, sub_path);
            ipld = match ipld {
                Ipld::Link(cid) => decode_ipld(&cid, self.get_block(&cid).await?.data())?,
                ipld => ipld,
            };
        }
        Ok(ipld)
    }
}

fn can_resolve(ipld: &Ipld, sub_path: &SubPath) -> bool {
    match sub_path {
        SubPath::Key(key) => {
            if let Ipld::Map(ref map) = ipld {
                if map.contains_key(key) {
                    return true;
                }
            }
        }
        SubPath::Index(index) => {
            if let Ipld::List(ref vec) = ipld {
                if *index < vec.len() {
                    return true;
                }
            }
        }
    }
    false
}

fn resolve(ipld: Ipld, sub_path: &SubPath) -> Ipld {
    match sub_path {
        SubPath::Key(key) => {
            if let Ipld::Map(mut map) = ipld {
                return map.remove(key).unwrap();
            }
        }
        SubPath::Index(index) => {
            if let Ipld::List(mut vec) = ipld {
                return vec.swap_remove(*index);
            }
        }
    }
    panic!(
        "Failed to resolved ipld: {:?} sub_path: {:?}",
        ipld, sub_path
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::create_mock_ipfs;
    use libipld::ipld;

    #[async_std::test]
    async fn test_resolve_root_cid() {
        let ipfs = create_mock_ipfs().await;
        let data = ipld!([1, 2, 3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let res = ipfs.get_dag(IpfsPath::from(cid)).await.unwrap();
        assert_eq!(res, data);
    }

    #[async_std::test]
    async fn test_resolve_array_elem() {
        let ipfs = create_mock_ipfs().await;
        let data = ipld!([1, 2, 3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let res = ipfs
            .get_dag(IpfsPath::from(cid).sub_path("1").unwrap())
            .await
            .unwrap();
        assert_eq!(res, ipld!(2));
    }

    #[async_std::test]
    async fn test_resolve_nested_array_elem() {
        let ipfs = create_mock_ipfs().await;
        let data = ipld!([1, [2], 3,]);
        let cid = ipfs.put_dag(data).await.unwrap();
        let res = ipfs
            .get_dag(IpfsPath::from(cid).sub_path("1/0").unwrap())
            .await
            .unwrap();
        assert_eq!(res, ipld!(2));
    }

    #[async_std::test]
    async fn test_resolve_object_elem() {
        let ipfs = create_mock_ipfs().await;
        let data = ipld!({
            "key": false,
        });
        let cid = ipfs.put_dag(data).await.unwrap();
        let res = ipfs
            .get_dag(IpfsPath::from(cid).sub_path("key").unwrap())
            .await
            .unwrap();
        assert_eq!(res, ipld!(false));
    }

    #[async_std::test]
    async fn test_resolve_cid_elem() {
        let ipfs = create_mock_ipfs().await;
        let data1 = ipld!([1]);
        let cid1 = ipfs.put_dag(data1).await.unwrap();
        let data2 = ipld!([cid1]);
        let cid2 = ipfs.put_dag(data2).await.unwrap();
        let res = ipfs
            .get_dag(IpfsPath::from(cid2).sub_path("0/0").unwrap())
            .await
            .unwrap();
        assert_eq!(res, ipld!(1));
    }
}
