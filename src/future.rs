use cid::Cid;
use crate::bitswap::Bitswap;
use crate::block::Block;
use crate::repo::Repo;
use futures::prelude::*;
use futures::try_ready;
use std::io::Error;
use std::sync::Arc;

pub struct BlockFuture {
    cid: Arc<Cid>,
    repo: Repo,
    bitswap: Bitswap,
}

impl BlockFuture {
    pub fn new(repo: Repo, bitswap: Bitswap, cid: Arc<Cid>) -> Self {
        BlockFuture {
            repo,
            bitswap,
            cid,
        }
    }
}

impl Future for BlockFuture {
    type Item = Block;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.repo.get(&self.cid).map_or_else(|| {
            try_ready!(self.bitswap.poll());
            Ok(Async::NotReady)
        }, |block| {
            Ok(Async::Ready(block))
        })
    }
}
