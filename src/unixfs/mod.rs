use crate::dag::IpldDag;
use crate::error::Error;
use crate::path::IpfsPath;
use crate::repo::{Repo, RepoTypes};
use async_std::fs;
use async_std::io::ReadExt;
use async_std::path::PathBuf;
use cid::{Cid, Codec};
use libipld::ipld::Ipld;
use libipld::pb::PbNode;
use std::collections::BTreeMap;
use std::convert::TryInto;

pub use ipfs_unixfs as ll;

mod cat;
pub use cat::{cat, TraversalFailed};

// No get provided at least as of now.

pub struct File {
    data: Vec<u8>,
}

impl File {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let mut file = fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(File { data })
    }

    pub async fn get_unixfs_v1<T: RepoTypes>(
        repo: &Repo<T>,
        path: IpfsPath,
    ) -> Result<Self, Error> {
        let ipld = repo.get_dag(path).await?;
        let pb_node: PbNode = (&ipld).try_into()?;
        Ok(File { data: pb_node.data })
    }

    pub async fn put_unixfs_v1<T: RepoTypes>(&self, repo: &Repo<T>) -> Result<Cid, Error> {
        let links: Vec<Ipld> = vec![];
        let mut pb_node = BTreeMap::<String, Ipld>::new();
        pb_node.insert("Data".to_string(), self.data.clone().into());
        pb_node.insert("Links".to_string(), links.into());
        repo.put_dag(pb_node.into(), Codec::DagProtobuf).await
    }
}

impl From<Vec<u8>> for File {
    fn from(data: Vec<u8>) -> Self {
        File { data }
    }
}

impl From<&str> for File {
    fn from(string: &str) -> Self {
        File {
            data: string.as_bytes().to_vec(),
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
    use crate::Node;
    use core::convert::TryFrom;

    #[async_std::test]
    async fn test_file_cid() {
        let Node { ipfs, bg_task: _bt } = Node::new("test_node").await;
        let file = File::from("\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}");
        let cid = Cid::try_from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        let cid2 = file.put_unixfs_v1(&ipfs.repo).await.unwrap();
        assert_eq!(cid.to_string(), cid2.to_string());
    }
}
