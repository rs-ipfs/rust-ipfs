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
    future: FutureObj<'static, Result<Option<Block>, std::io::Error>>,
}

impl<TBlockStore: BlockStore> BlockFuture<TBlockStore> {
    pub fn new(block_store: TBlockStore, cid: Cid) -> Self {
        let future = block_store.get(&cid);
        BlockFuture {
            block_store,
            cid,
            future,
        }
    }
}

impl<TBlockStore: BlockStore> Future for BlockFuture<TBlockStore> {
    type Output = Result<Block, std::io::Error>;

    fn poll(mut self: Pin<&mut Self>, waker: &Waker) -> Poll<Self::Output> {
        return match self.future.poll_unpin(waker) {
            Poll::Ready(Ok(Some(block))) => Poll::Ready(Ok(block)),
            Poll::Ready(Ok(None)) => {
                let future = self.block_store.get(&self.cid);
                self.get_mut().future = future;
                tokio::prelude::task::current().notify();
                //waker.wake();
                Poll::Pending
            },
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(err))
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
