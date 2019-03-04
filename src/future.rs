use crate::block::{Block, Cid};
use crate::error::Error;
use crate::repo::BlockStore;
use futures::FutureExt;
use futures::compat::Compat;
use futures::future::FutureObj;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Waker};

pub fn tokio_run<F: Future<Output=()> + Send + 'static>(future: F) {
    tokio::run(Compat::new(Box::pin(
        future.map(|()| -> Result<(), ()> { Ok(()) })
    )));
}

pub fn tokio_spawn<F: Future<Output=()> + Send + 'static>(future: F) {
    tokio::spawn(Compat::new(Box::pin(
        future.map(|()| -> Result<(), ()> { Ok(()) })
    )));
}

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
