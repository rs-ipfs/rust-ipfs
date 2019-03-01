use crate::block::Cid;
use crate::ipld::{Ipld, IpldError, IpldPath, SubPath};
use crate::repo::{Repo, RepoTypes};
use core::future::Future;

pub struct IpldDag<Types: RepoTypes> {
    repo: Repo<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    pub fn new(repo: Repo<Types>) -> Self {
        IpldDag {
            repo,
        }
    }

    pub fn put(&self, data: Ipld) -> impl Future<Output=Result<Cid, IpldError>> {
        let repo = self.repo.clone();
        async move {
            let block = data.to_dag_cbor()?;
            let cid = await!(repo.put_block(block))?;
            Ok(cid)
        }
    }

    pub fn get(&self, path: IpldPath) -> impl Future<Output=Result<Ipld, IpldError>> {
        let repo = self.repo.clone();
        async move {
            let mut ipld = Ipld::from(&await!(repo.get_block(path.root()))?)?;
            for sub_path in path.iter() {
                if !can_resolve(&ipld, sub_path) {
                    let path = sub_path.to_owned();
                    return Err(IpldError::ResolveError { ipld, path });
                }
                ipld = resolve(ipld, sub_path);
                ipld = match ipld {
                    Ipld::Cid(cid) => {
                        Ipld::from(&await!(repo.get_block(&cid))?)?
                    }
                    ipld => ipld,
                };
            }
            Ok(ipld)
        }
    }
}

fn can_resolve(ipld: &Ipld, sub_path: &SubPath) -> bool {
    match sub_path {
        SubPath::Key(key) => {
            if let Ipld::Object(ref map) = ipld {
                if map.contains_key(key) {
                    return true;
                }
            }
        }
        SubPath::Index(index) => {
            if let Ipld::Array(ref vec) = ipld {
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
            if let Ipld::Object(mut map) = ipld {
                return map.remove(key).unwrap()
            }
        }
        SubPath::Index(index) => {
            if let Ipld::Array(mut vec) = ipld {
                return vec.swap_remove(*index)
            }
        }
    }
    panic!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tests::create_mock_repo;
    use crate::future::tokio_run;
    use std::collections::HashMap;

    #[test]
    fn test_resolve_root_cid() {
        tokio_run(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::U64(2), Ipld::U64(3)]);
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::new(cid);
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, data);

        });
    }

    #[test]
    fn test_resolve_array_elem() {
        tokio_run(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data: Ipld = vec![1, 2, 3].into();
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::from(cid, "1").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Ipld::U64(2));
        });
    }

    #[test]
    fn test_resolve_nested_array_elem() {
        tokio_run(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::Array(vec![Ipld::U64(2)]), Ipld::U64(3)]);
            let cid = await!(dag.put(data.clone())).unwrap();

            let path = IpldPath::from(cid, "1/0").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Ipld::U64(2));
        });
    }

    #[test]
    fn test_resolve_object_elem() {
        tokio_run(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let mut data = HashMap::new();
            data.insert("key", false);
            let cid = await!(dag.put(data.into())).unwrap();

            let path = IpldPath::from(cid, "key").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Ipld::Bool(false));
        });
    }

    #[test]
    fn test_resolve_cid_elem() {
        tokio_run(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data1 = vec![1].into();
            let cid1 = await!(dag.put(data1)).unwrap();
            let data2 = vec![cid1].into();
            let cid2 = await!(dag.put(data2)).unwrap();

            let path = IpldPath::from(cid2, "0/0").unwrap();
            let res = await!(dag.get(path)).unwrap();
            assert_eq!(res, Ipld::U64(1));
        });
    }
}
