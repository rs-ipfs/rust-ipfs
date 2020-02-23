use crate::error::Error;
use crate::ipld::{Ipld, IpldDag, formats::pb::PbNode};
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use std::collections::HashMap;
use std::convert::TryInto;
use async_std::path::PathBuf;
use async_std::fs;
use async_std::io::ReadExt;

pub struct File {
    data: Vec<u8>,
}

impl File {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let mut file = fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(File {
            data
        })
    }

    pub async fn get_unixfs_v1<T: RepoTypes>(dag: &IpldDag<T>, path: IpfsPath) -> Result<Self, Error> {
        let ipld = dag.get(path).await?;
        let pb_node: PbNode = match ipld.try_into() {
            Ok(pb_node) => pb_node,
            Err(_) => bail!("invalid dag_pb node"),
        };
        Ok(File {
            data: pb_node.data,
        })
    }

    pub async fn put_unixfs_v1<T: RepoTypes>(&self, dag: &IpldDag<T>) -> Result<IpfsPath, Error> {
        let links: Vec<Ipld> = vec![];
        let mut pb_node = HashMap::<&str, Ipld>::new();
        pb_node.insert("Data", self.data.clone().into());
        pb_node.insert("Links", links.into());
        let ipld = pb_node.into();
        dag.put(ipld, cid::Codec::DagProtobuf).await
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
    use crate::tests::async_test;

    #[test]
    fn test_file_cid() {
        let repo = create_mock_repo();
        let dag = IpldDag::new(repo);
        let file = File::from("\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}");
        let cid = Cid::from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        async_test(async move {
            let path = file.put_unixfs_v1(&dag).await.unwrap();
            assert_eq!(cid.to_string(), path.root().cid().unwrap().to_string());
        });
    }
}
