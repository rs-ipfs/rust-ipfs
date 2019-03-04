#![allow(dead_code)]
use crate::error::Error;
use crate::path::{IpfsPath, PathRoot};
use crate::repo::{Repo, RepoTypes};
use libp2p::PeerId;
use std::future::Future;

mod ipns_pb;
mod entry;

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
            if path.root().is_ipns() {
                Ok(await!(repo.get_ipns(path.root().peer_id()?))??)
            } else {
                Ok(path)
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
