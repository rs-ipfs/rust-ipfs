/*use crate::block::{Block, Cid};
use crate::error::Error;
use crate::repo::BlockStore;
use std::future::Future;
use futures::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Context};

pub struct BlockFuture<TBlockStore: BlockStore> {
    block_store: TBlockStore,
    cid: Cid,
    future: FutureObj<'static, Result<Option<Block>, Error>>,
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
    type Output = Result<Block, Error>;

    fn poll(mut self: Pin<&mut Self>, context: &Context) -> Poll<Self::Output> {
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
}*/
