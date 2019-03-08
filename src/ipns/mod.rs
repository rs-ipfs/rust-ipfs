#![allow(dead_code)]
use crate::error::Error;
use crate::path::{IpfsPath, PathRoot};
use crate::repo::{Repo, RepoTypes};
use libp2p::PeerId;
use std::future::Future;

mod dns;
mod entry;
mod ipns_pb;

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
    pub fn resolve(&self, path: &IpfsPath) ->
    impl Future<Output=Result<IpfsPath, Error>>
    {
        let repo = self.repo.clone();
        let path = path.to_owned();
        async move {
            match path.root() {
                PathRoot::Ipld(_) => Ok(path),
                PathRoot::Ipns(peer_id) => {
                    match await!(repo.get_ipns(peer_id))? {
                        Some(path) => Ok(path),
                        None => bail!("unimplemented"),
                    }
                },
                PathRoot::Dns(domain) => {
                    Ok(await!(dns::resolve(domain)?)?)
                },
            }
        }
    }

    /// Publishes an ipld path.
    pub fn publish(&self, key: &PeerId, path: &IpfsPath) ->
    impl Future<Output=Result<IpfsPath, Error>>
    {
        let future = self.repo.put_ipns(key, path);
        let key = key.to_owned();
        let mut path = path.to_owned();
        async move {
            await!(future)?;
            path.set_root(PathRoot::Ipns(key));
            Ok(path)
        }
    }

    /// Cancel an ipns path.
    pub fn cancel(&self, key: &PeerId) ->
    impl Future<Output=Result<(), Error>>
    {
        self.repo.remove_ipns(key)
    }
}
