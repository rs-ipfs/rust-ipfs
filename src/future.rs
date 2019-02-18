use crate::block::{Block, Cid};
use crate::repo::BlockStore;
use futures::prelude::*;

pub struct BlockFuture<TBlockStore: BlockStore> {
    block_store: TBlockStore,
    cid: Cid,
}

impl<TBlockStore: BlockStore> BlockFuture<TBlockStore> {
    pub fn new(block_store: TBlockStore, cid: Cid) -> Self {
        BlockFuture {
            block_store,
            cid,
        }
    }
}

impl<TBlockStore: BlockStore> Future for BlockFuture<TBlockStore> {
    type Item = Block;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, ()> {
        self.block_store.get(&self.cid).map_or_else(|| {
            Ok(Async::NotReady)
        }, |block| {
            Ok(Async::Ready(block))
        })
    }
}
