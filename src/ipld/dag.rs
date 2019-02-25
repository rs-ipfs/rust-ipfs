use crate::block::Cid;
use crate::ipld::{Ipld, IpldError, IpldPath, SubPath};
use crate::repo::{Repo, RepoTypes, BlockStore};
use futures::future::FutureObj;

pub struct IpldDag<Types: RepoTypes> {
    repo: Repo<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    pub fn new(repo: Repo<Types>) -> Self {
        IpldDag {
            repo,
        }
    }

    pub fn put(&self, data: Ipld) -> FutureObj<'static, Result<Cid, IpldError>> {
        let block_store = self.repo.block_store.clone();
        FutureObj::new(Box::new(async move {
            let block = data.to_dag_cbor()?;
            let cid = await!(block_store.put(block))?;
            Ok(cid)
        }))
    }

    pub fn get(&self, path: IpldPath) -> FutureObj<'static, Result<Option<Ipld>, IpldError>> {
        let block_store = self.repo.block_store.clone();
        FutureObj::new(Box::new(async move {
            let mut ipld = match await!(block_store.get(path.root()))? {
                Some(block) => Some(Ipld::from(&block)?),
                None => None,
            };
            for sub_path in path.iter() {
                if ipld.is_none() {
                    return Ok(None);
                }
                let ipld_owned = ipld.take().unwrap();
                let new_ipld = match sub_path {
                    SubPath::Key(ref key) => {
                        if let Ipld::Object(mut map) = ipld_owned {
                            map.remove(key)
                        } else {
                            None
                        }
                    }
                    SubPath::Index(index) => {
                        if let Ipld::Array(mut vec) = ipld_owned {
                            if *index < vec.len() {
                                Some(vec.swap_remove(*index))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };
                ipld = match new_ipld {
                    Some(Ipld::Cid(cid)) => {
                        match await!(block_store.get(cid))? {
                            Some(block) => Some(Ipld::from(&block)?),
                            None => None,
                        }
                    }
                    _ => new_ipld,
                };
            }
            Ok(ipld)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tests::create_mock_repo;
    use std::collections::HashMap;

    #[test]
    fn test_resolve_root_cid() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::U64(2), Ipld::U64(3)]);
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::new(cid);
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Some(data));

        });
    }

    #[test]
    fn test_resolve_array_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data: Ipld = vec![1, 2, 3].into();
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::from(cid, "1").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Some(Ipld::U64(2)));
        });
    }

    #[test]
    fn test_resolve_nested_array_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::Array(vec![Ipld::U64(2)]), Ipld::U64(3)]);
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::from(cid, "1/0").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Some(Ipld::U64(2)));
        });
    }

    #[test]
    fn test_resolve_object_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let mut data = HashMap::new();
            data.insert("key", false);
            let cid = await!(dag.put(data.into())).unwrap();

            let path = IpldPath::from(cid, "key").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Some(Ipld::Bool(false)));
        });
    }

    #[test]
    fn test_resolve_cid_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data1 = vec![1].into();
            let cid1 = await!(dag.put(data1)).unwrap();
            let data2 = vec![cid1].into();
            let cid2 = await!(dag.put(data2)).unwrap();

            let path = IpldPath::from(cid2, "0/0").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Some(Ipld::U64(1)));
        });
    }
}
