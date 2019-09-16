use crate::error::Error;
use crate::ipld::Ipld;
use crate::path::{IpfsPath, IpfsPathError, PathRoot, SubPath};
use crate::repo::{Repo, RepoTypes};
use cid::Codec;
use core::future::Future;
use failure::bail;

#[derive(Clone)]
pub struct IpldDag<Types: RepoTypes> {
    repo: Repo<Types>,
}

impl<Types: RepoTypes> IpldDag<Types> {
    pub fn new(repo: Repo<Types>) -> Self {
        IpldDag {
            repo,
        }
    }

    pub fn put(&self, data: Ipld, codec: Codec) ->
    impl Future<Output=Result<IpfsPath, Error>>
    {
        let repo = self.repo.clone();
        async move {
            let block = data.to_block(codec)?;
            let cid = repo.put_block(block).await?;
            Ok(IpfsPath::new(PathRoot::Ipld(cid)))
        }
    }

    pub fn get(&self, path: IpfsPath) -> impl Future<Output=Result<Ipld, failure::Error>> {
        let repo = self.repo.clone();
        async move {
            let cid = match path.root().cid() {
                Some(cid) => cid,
                None => bail!("expected cid"),
            };
            let mut ipld = Ipld::from(&repo.get_block(&cid).await?)?;
            for sub_path in path.iter() {
                if !can_resolve(&ipld, sub_path) {
                    let path = sub_path.to_owned();
                    return Err(IpfsPathError::ResolveError { ipld, path }.into());
                }
                ipld = resolve(ipld, sub_path);
                ipld = match ipld {
                    Ipld::Link(root) => {
                        match root.cid() {
                            Some(cid) => Ipld::from(&repo.get_block(cid).await?)?,
                            None => bail!("expected cid"),
                        }
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
    use std::collections::HashMap;

    #[test]
    fn test_resolve_root_cid() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::U64(2), Ipld::U64(3)]);
            let path = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();

            let res = dag.get(path).await.unwrap();
            assert_eq!(res, data);

        });
    }

    #[test]
    fn test_resolve_array_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data: Ipld = vec![1, 2, 3].into();
            let path = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
            let res = dag.get(path.sub_path("1").unwrap()).await.unwrap();
            assert_eq!(res, Ipld::U64(2));
        });
    }

    #[test]
    fn test_resolve_nested_array_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data = Ipld::Array(vec![Ipld::U64(1), Ipld::Array(vec![Ipld::U64(2)]), Ipld::U64(3)]);
            let path = dag.put(data.clone(), Codec::DagCBOR).await.unwrap();
            let res = dag.get(path.sub_path("1/0").unwrap()).await.unwrap();
            assert_eq!(res, Ipld::U64(2));
        });
    }

    #[test]
    fn test_resolve_object_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let mut data = HashMap::new();
            data.insert("key", false);
            let path = dag.put(data.into(), Codec::DagCBOR).await.unwrap();
            let res = dag.get(path.sub_path("key").unwrap()).await.unwrap();
            assert_eq!(res, Ipld::Bool(false));
        });
    }

    #[test]
    fn test_resolve_cid_elem() {
        tokio::run_async(async {
            let repo = create_mock_repo();
            let dag = IpldDag::new(repo);
            let data1 = vec![1].into();
            let path1 = dag.put(data1, Codec::DagCBOR).await.unwrap();
            let data2 = vec![path1.root().to_owned()].into();
            let path = dag.put(data2, Codec::DagCBOR).await.unwrap();
            let res = dag.get(path.sub_path("0/0").unwrap()).await.unwrap();
            assert_eq!(res, Ipld::U64(1));
        });
    }
}
