#![allow(dead_code)]
use crate::error::Error;
use crate::path::{IpfsPath, PathRoot};
use crate::repo::{Repo, RepoTypes};
use async_trait::async_trait;
use libp2p::PeerId;

mod dns;
mod entry;
mod ipns_pb {
    include!(concat!(env!("OUT_DIR"), "/ipns_pb.rs"));
}

#[async_trait]
pub trait Ipns {
    async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error>;

    async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error>;

    async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error>;
}

#[async_trait]
impl<Types: RepoTypes> Ipns for Repo<Types> {
    /// Resolves a ipns path to an ipld path.
    async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            PathRoot::Ipns(peer_id) => match self.get_ipns(peer_id).await? {
                Some(path) => Ok(path),
                None => Err(anyhow::anyhow!("unimplemented")),
            },
            PathRoot::Dns(domain) => Ok(dns::resolve(domain).await?),
        }
    }

    /// Publishes an ipld path.
    async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let future = self.put_ipns(key, path);
        let key = key.to_owned();
        let mut path = path.to_owned();
        future.await?;
        path.set_root(PathRoot::Ipns(key));
        Ok(path)
    }

    /// Cancel an ipns path.
    async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error> {
        self.remove_ipns(key).await?;
        Ok(())
    }
}
