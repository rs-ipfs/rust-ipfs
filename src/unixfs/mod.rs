use crate::error::Error;
use crate::ipld::{Ipld, IpldDag, formats::pb::PbNode};
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use core::future::Future;
use futures::compat::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;

pub struct File {
    data: Vec<u8>,
}

impl File {
    pub fn new(path: PathBuf) -> impl Future<Output=Result<Self, Error>> {
        async move {
            let file = await!(tokio::fs::File::open(path).compat())?;
            let (_, data) = await!(tokio::io::read_to_end(file, Vec::new()).compat())?;
            Ok(File {
                data
            })
        }
    }

    pub fn get_unixfs_v1<T: RepoTypes>(dag: &IpldDag<T>, path: IpfsPath) ->
    impl Future<Output=Result<Self, Error>> {
        let future = dag.get(path);
        async move {
            let ipld = await!(future)?;
            let pb_node: PbNode = ipld.try_into()?;
            Ok(File {
                data: pb_node.data,
            })
        }
    }

    pub fn put_unixfs_v1<T: RepoTypes>(&self, dag: &IpldDag<T>) ->
    impl Future<Output=Result<IpfsPath, Error>>
    {
        let links: Vec<Ipld> = vec![];
        let mut pb_node = HashMap::<&str, Ipld>::new();
        pb_node.insert("Data", self.data.clone().into());
        pb_node.insert("Links", links.into());
        let ipld = pb_node.into();
        dag.put(ipld, cid::Codec::DagProtobuf)
    }
}

impl From<Vec<u8>> for File {
    fn from(data: Vec<u8>) -> Self {
        File {
            data,
        }
    }
}

impl From<&str> for File {
    fn from(string: &str) -> Self {
        File {
            data: string.as_bytes().to_vec()
        }
    }
}

impl Into<String> for File {
    fn into(self) -> String {
        String::from_utf8_lossy(&self.data).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Cid;
    use crate::repo::tests::create_mock_repo;

    #[test]
    fn test_file_cid() {
        let repo = create_mock_repo();
        let dag = IpldDag::new(repo);
        let file = File::from("\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}");
        let cid = Cid::from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        tokio::run_async(async move {
            let path = await!(file.put_unixfs_v1(&dag)).unwrap();
            assert_eq!(cid.to_string(), path.root().cid().unwrap().to_string());
        });
    }
}
