use crate::block::{Block, Cid};
use crate::repo::Repo;
use futures::prelude::*;
use std::io::Error;

pub struct BlockFuture {
    repo: Repo,
    cid: Cid,
}

impl BlockFuture {
    pub fn new(repo: Repo, cid: Cid) -> Self {
        BlockFuture {
            repo,
            cid,
        }
    }
}

impl Future for BlockFuture {
    type Item = Block;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.repo.get(&self.cid).map_or_else(|| {
            Ok(Async::NotReady)
        }, |block| {
            Ok(Async::Ready(block))
        })
    }
}
