use crate::block::{Block, Cid};
use crate::repo::BlockStore;
use futures::future::FutureObj;
use futures::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Waker};

pub struct BlockFuture<TBlockStore: BlockStore> {
    block_store: TBlockStore,
    cid: Cid,
    future: FutureObj<'static, Option<Block>>,
}

impl<TBlockStore: BlockStore> BlockFuture<TBlockStore> {
    pub fn new(block_store: TBlockStore, cid: Cid) -> Self {
        let future = block_store.get(cid.clone());
        BlockFuture {
            block_store,
            cid,
            future,
        }
    }
}

impl<TBlockStore: BlockStore> Future for BlockFuture<TBlockStore> {
    type Output = Block;

    fn poll(mut self: Pin<&mut Self>, waker: &Waker) -> Poll<Self::Output> {
        return match self.future.poll_unpin(waker) {
            Poll::Ready(Some(block)) => Poll::Ready(block),
            Poll::Ready(None) => {
                let future = self.block_store.get(self.cid.clone());
                self.get_mut().future = future;
                waker.wake();
                Poll::Pending
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
