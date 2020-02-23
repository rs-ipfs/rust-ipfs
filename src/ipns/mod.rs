#![allow(dead_code)]
use crate::error::Error;
use crate::path::{IpfsPath, PathRoot};
use crate::repo::{Repo, RepoTypes};
use libp2p::PeerId;

mod dns;
mod entry;
mod ipns_pb {
    include!(concat!(env!("OUT_DIR"), "/ipns_pb.rs"));
}

pub struct Ipns<Types: RepoTypes> {
    repo: Repo<Types>,
}

impl<Types: RepoTypes> Ipns<Types> {
    pub fn new(repo: Repo<Types>) -> Self {
        Ipns {
            repo
        }
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve(&self, path: &IpfsPath) -> Result<IpfsPath, Error>
    {
        let mut repo = self.repo.clone();
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            PathRoot::Ipns(peer_id) => {
                match repo.get_ipns(peer_id).await? {
                    Some(path) => Ok(path),
                    None => bail!("unimplemented"),
                }
            },
            PathRoot::Dns(domain) => {
                Ok(dns::resolve(domain).await?)
            },
        }
    }

    /// Publishes an ipld path.
    pub async fn publish(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error>
    {
        let future = self.repo.put_ipns(key, path);
        let key = key.to_owned();
        let mut path = path.to_owned();
        future.await?;
        path.set_root(PathRoot::Ipns(key));
        Ok(path)
    }

    /// Cancel an ipns path.
    pub async fn cancel(&self, key: &PeerId) -> Result<(), Error>
    {
        self.repo.remove_ipns(key).await?;
        Ok(())
    }
}
