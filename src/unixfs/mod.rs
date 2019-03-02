use crate::block::Cid;
use crate::error::Error;
use crate::ipld::{Ipld, IpldDag};
use crate::repo::RepoTypes;
use core::future::Future;
use std::collections::HashMap;

pub struct File {
    data: Vec<u8>,
}

impl File {
    pub fn put_unixfs_v1<T: RepoTypes>(&self, dag: &IpldDag<T>) ->
    impl Future<Output=Result<Cid, Error>>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tests::create_mock_repo;

    #[test]
    fn test_file_cid() {
        let repo = create_mock_repo();
        let dag = IpldDag::new(repo);
        let file = File::from("Here is some data\n");
        let cid = Cid::from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        tokio::run_async(async move {
            let cid2 = await!(file.put_unixfs_v1(&dag)).unwrap();
            assert_eq!(cid, cid2);
        });
    }
}
