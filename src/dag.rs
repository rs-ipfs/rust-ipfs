use crate::block::{Block, Cid};
use crate::error::Error;
use crate::path::{IpfsPath, IpfsPathError, SubPath};
use crate::repo::{Repo, RepoTypes};
use libipld::block::{decode_ipld, encode_ipld};
use libipld::cid::{Codec, Version};
use libipld::ipld::Ipld;

#[derive(Clone, Debug)]
pub struct IpldDag<Types: RepoTypes> {
    repo: Repo<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    pub fn new(repo: Repo<Types>) -> Self {
        IpldDag { repo }
    }

    pub async fn put(&self, data: Ipld, codec: Codec) -> Result<Cid, Error> {
        let mut repo = self.repo.clone();
        let bytes = encode_ipld(&data, codec)?;
        let hash = multihash::Sha2_256::digest(&bytes);
        let version = if codec == Codec::DagProtobuf {
            Version::V0
        } else {
            Version::V1
        };
        let cid = Cid::new(version, codec, hash)?;
        let block = Block::new(bytes, cid);
        let cid = repo.put_block(block).await?;
        Ok(cid)
    }

    pub async fn get(&self, path: IpfsPath) -> Result<Ipld, Error> {
        let mut repo = self.repo.clone();
        let cid = match path.root().cid() {
            Some(cid) => cid,
            None => bail!("expected cid"),
        };
        let mut ipld = decode_ipld(&cid, repo.get_block(&cid).await?.data())?;
        for sub_path in path.iter() {
            if !can_resolve(&ipld, sub_path) {
                let path = sub_path.to_owned();
                return Err(IpfsPathError::ResolveError { ipld, path }.into());
            }
            ipld = resolve(ipld, sub_path);
            ipld = match ipld {
                Ipld::Link(cid) => decode_ipld(&cid, repo.get_block(&cid).await?.data())?,
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
    use crate::repo::tests::create_mock_repo;
    use crate::tests::async_test;
    use libipld::ipld;

    #[test]
    fn test_resolve_root_cid() {
        async_test(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = ipld!([1, 2, 3]);
            let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
            let res = dag.get(IpfsPath::from(cid)).await.unwrap();
            assert_eq!(res, data);
        });
    }

    #[test]
    fn test_resolve_array_elem() {
        async_test(async move {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = ipld!([1, 2, 3]);
            let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
            let res = dag
                .get(IpfsPath::from(cid).sub_path("1").unwrap())
                .await
                .unwrap();
            assert_eq!(res, ipld!(2));
        });
    }

    #[test]
    fn test_resolve_nested_array_elem() {
        async_test(async move {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = ipld!([1, [2], 3,]);
            let cid = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
            let res = dag
                .get(IpfsPath::from(cid).sub_path("1/0").unwrap())
                .await
                .unwrap();
            assert_eq!(res, ipld!(2));
        });
    }

    #[test]
    fn test_resolve_object_elem() {
        async_test(async move {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = ipld!({
                "key": false,
            });
            let cid = dag.put(data.into(), Codec::DagCBOR).await.unwrap();
            let res = dag
                .get(IpfsPath::from(cid).sub_path("key").unwrap())
                .await
                .unwrap();
            assert_eq!(res, ipld!(false));
        });
    }

    #[test]
    fn test_resolve_cid_elem() {
        async_test(async move {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data1 = ipld!([1]);
            let cid1 = dag.put(data1, Codec::DagCBOR).await.unwrap();
            let data2 = ipld!([cid1]);
            let cid2 = dag.put(data2, Codec::DagCBOR).await.unwrap();
            let res = dag
                .get(IpfsPath::from(cid2).sub_path("0/0").unwrap())
                .await
                .unwrap();
            assert_eq!(res, ipld!(1));
        });
    }
}
