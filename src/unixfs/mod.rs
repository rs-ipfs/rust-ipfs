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
use ipfs_unixfs::file::{visit::IdleFileVisit, FileReadFailed};
use std::fmt;
use std::ops::Range;
use std::borrow::Borrow;

/// IPFS cat operation, producing a stream of file bytes. This is generic over the different kinds
/// of ways to own an `Ipfs` value in order to support both operating with borrowed `Ipfs` value
/// and an owned value. Passing an owned value allows the return value to be `'static`, which can
/// be helpful in some contexts, like the http.
///
/// Returns a stream of bytes on the file pointed with the Cid.
pub fn cat<'a, Types, MaybeOwned>(
    ipfs: MaybeOwned,
    cid: Cid,
    range: Option<Range<u64>>,
) -> impl Stream<Item = Result<Vec<u8>, TraversalFailed>> + Send + 'a
    where Types: IpfsTypes,
          MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    use bitswap::Block;

    // using async_stream here at least to get on faster; writing custom streams is not too easy
    // but this might be easy enough to write open.
    stream! {
        let mut visit = IdleFileVisit::default();
        if let Some(range) = range {
            visit = visit.with_target_range(range);
        }

        // Get the root block to start the traversal. The stream does not expose any of the file
        // metadata. To get to it the user needs to create a Visitor over the first block.
        let borrow = ipfs.borrow();
        let Block { cid, data } = match borrow.get_block(&cid).await {
            Ok(block) => block,
            Err(e) => {
                yield Err(TraversalFailed::Loading(cid, e));
                return;
            }
        };

        // Start the visit from the root block.
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
            let (next, _) = visit.pending_links();

            let borrow = ipfs.borrow();
            let Block { cid, data } = match borrow.get_block(&next).await {
                Ok(block) => block,
                Err(e) => {
                    yield Err(TraversalFailed::Loading(next.to_owned(), e));
                    return;
                },
            };

            match visit.continue_walk(&data) {
                Ok((bytes, next_visit)) => {
                    if !bytes.is_empty() {
                        // TODO: manual implementation could allow returning just the slice
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

impl std::error::Error for TraversalFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use TraversalFailed::*;

        match self {
            Loading(_, _) => {
                // FIXME: anyhow::Error cannot be given out as source.
                None
            }
            Walking(_, e) => Some(e),
        }
    }
}

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
}
