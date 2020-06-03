use crate::dag::IpldDag;
use crate::error::Error;
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use async_std::fs;
use async_std::io::ReadExt;
use async_std::path::PathBuf;
use libipld::cid::{Cid, Codec};
use libipld::ipld::Ipld;
use libipld::pb::PbNode;
use std::collections::BTreeMap;
use std::convert::TryInto;

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
        dag: &IpldDag<T>,
        path: IpfsPath,
    ) -> Result<Self, Error> {
        let ipld = dag.get(path).await?;
        let pb_node: PbNode = (&ipld).try_into()?;
        Ok(File { data: pb_node.data })
    }

    pub async fn put_unixfs_v1<T: RepoTypes>(&self, dag: &IpldDag<T>) -> Result<Cid, Error> {
        let links: Vec<Ipld> = vec![];
        let mut pb_node = BTreeMap::<String, Ipld>::new();
        pb_node.insert("Data".to_string(), self.data.clone().into());
        pb_node.insert("Links".to_string(), links.into());
        dag.put(pb_node.into(), Codec::DagProtobuf).await
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

use crate::{Ipfs, IpfsTypes};
use async_stream::stream;
use futures::stream::Stream;
use ipfs_unixfs::file::{
    visit::IdleFileVisit,
    FileReadFailed,
};
use std::fmt;
use std::ops::Range;

pub fn cat(
    ipfs: Ipfs<impl IpfsTypes>,
    cid: Cid,
    range: Option<Range<u64>>,
) -> impl Stream<Item = Result<Vec<u8>, TraversalFailed>> + Send + 'static {
    use bitswap::Block;

    // using async_stream here at least to get on faster; writing custom streams is not too easy
    // but this might be easy enough to write open.
    stream! {
        let mut visit = IdleFileVisit::default();
        if let Some(range) = range {
            visit = visit.with_target_range(range);
        }

        let Block { cid, data } = match ipfs.get_block(&cid).await {
            Ok(block) => block,
            Err(e) => {
                yield Err(TraversalFailed::Loading(cid, e));
                return;
            }
        };

        let mut visit = match visit.start(&data) {
            Ok((bytes, _, visit)) => {
                if !bytes.is_empty() {
                    yield Ok(bytes.to_vec());
                }

                match visit {
                    Some(v) => v,
                    None => return,
                }
            },
            Err(e) => {
                yield Err(TraversalFailed::Walking(cid, e));
                return;
            }
        };

        loop {
            // TODO: if it was possible, it would make sense to start downloading N of these
            // TODO: it might be reasonable to provide (&Cid, impl Iterator<Item = &Cid>) here and
            // move the unwrap into the library if this all turns out to be a good idea.
            let next = visit.pending_links().next()
                .expect("there must be links, otherwise visitation would had not continued");

            let Block { cid, data } = match ipfs.get_block(&next).await {
                Ok(block) => block,
                Err(e) => {
                    yield Err(TraversalFailed::Loading(next.to_owned(), e));
                    return;
                },
            };

            match visit.continue_walk(&data) {
                Ok((bytes, next_visit)) => {
                    if !bytes.is_empty() {
                        yield Ok(bytes.to_vec());
                    }

                    match next_visit {
                        Some(v) => visit = v,
                        None => return,
                    }
                }
                Err(e) => {
                    yield Err(TraversalFailed::Walking(cid, e));
                    return;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum TraversalFailed {
    Loading(Cid, Error),
    Walking(Cid, FileReadFailed),
}

impl fmt::Display for TraversalFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TraversalFailed::*;
        match self {
            Loading(cid, e) => write!(fmt, "loading of {} failed: {}", cid, e),
            Walking(cid, e) => write!(fmt, "failed to walk {}: {}", cid, e),
        }
    }
}

impl std::error::Error for TraversalFailed {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tests::create_mock_repo;
    use core::convert::TryFrom;

    #[async_std::test]
    async fn test_file_cid() {
        let (repo, _) = create_mock_repo();
        let dag = IpldDag::new(repo);
        let file = File::from("\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}");
        let cid = Cid::try_from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        let cid2 = file.put_unixfs_v1(&dag).await.unwrap();
        assert_eq!(cid.to_string(), cid2.to_string());
    }

    #[ignore]
    #[async_std::test]
    async fn cat() {
        use crate::{IpfsOptions, UninitializedIpfs};
        use async_std::task;
        use futures::stream::StreamExt;
        use hex_literal::hex;
        use sha2::{Digest, Sha256};
        use std::time::{Duration, Instant};

        let options = IpfsOptions::inmemory_with_generated_keys(false);

        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        let id = ipfs.identity().await.unwrap();
        println!("listening at: {}/p2p/{}", id.1[0], id.0.into_peer_id());
        let cid = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        println!("please connect an go-ipfs with Cid {}", cid);

        while ipfs.peers().await.unwrap().is_empty() {
            async_std::future::timeout(
                Duration::from_millis(100),
                futures::future::pending::<()>(),
            )
            .await
            .unwrap_err();
        }

        println!("got peer");
        let started_at = Instant::now();

        let stream = super::cat(ipfs, libipld::cid::Cid::try_from(cid).unwrap(), None);

        futures::pin_mut!(stream);

        let mut digest = Sha256::new();
        let mut count = 0;

        loop {
            let bytes = match stream.next().await {
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => panic!("got error: {}", e),
                None => break,
            };

            digest.input(&bytes);
            count += bytes.len();
        }

        let result = digest.result();
        let elapsed = started_at.elapsed();

        println!("elapsed: {:?}", elapsed);

        assert_eq!(count, 111_812_744);
        assert_eq!(
            &result[..],
            hex!("33763f3541711e39fa743da45ff9512d54ade61406173f3d267ba4484cec7ea3")
        );
    }
}
