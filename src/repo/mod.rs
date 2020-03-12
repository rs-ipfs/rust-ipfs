//! IPFS repo
use crate::error::Error;
use crate::path::IpfsPath;
use crate::IpfsOptions;
use async_std::path::PathBuf;
use async_trait::async_trait;
use bitswap::Block;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use crossbeam::{Receiver, Sender};
use libipld::cid::Cid;
use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub mod fs;
pub mod mem;

pub trait RepoTypes: Clone + Send + Sync + 'static {
    type TBlockStore: BlockStore;
    type TDataStore: DataStore;
}

#[derive(Clone, Debug)]
pub struct RepoOptions<TRepoTypes: RepoTypes> {
    _marker: PhantomData<TRepoTypes>,
    path: PathBuf,
}

impl<TRepoTypes: RepoTypes> From<&IpfsOptions<TRepoTypes>> for RepoOptions<TRepoTypes> {
    fn from(options: &IpfsOptions<TRepoTypes>) -> Self {
        RepoOptions {
            _marker: PhantomData,
            path: options.ipfs_path.clone(),
        }
    }
}

pub fn create_repo<TRepoTypes: RepoTypes>(
    options: RepoOptions<TRepoTypes>,
) -> (Arc<Repo<TRepoTypes>>, Receiver<RepoEvent>) {
    let (repo, ch) = Repo::new(options);
    (Arc::new(repo), ch)
}

#[async_trait]
pub trait BlockStore: Debug + Clone + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    async fn put(&self, block: Block) -> Result<Cid, Error>;
    async fn remove(&self, cid: &Cid) -> Result<(), Error>;
}

#[async_trait]
pub trait DataStore: Debug + Clone + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error>;
    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum Column {
    Ipns,
}

#[derive(Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    block_store: TRepoTypes::TBlockStore,
    data_store: TRepoTypes::TDataStore,
    events: Sender<RepoEvent>,
    subscriptions: Mutex<HashMap<Cid, Arc<Mutex<Subscription>>>>,
}

#[derive(Clone, Debug)]
pub enum RepoEvent {
    WantBlock(Cid),
    ProvideBlock(Cid),
    UnprovideBlock(Cid),
}

impl<TRepoTypes: RepoTypes> Repo<TRepoTypes> {
    pub fn new(options: RepoOptions<TRepoTypes>) -> (Self, Receiver<RepoEvent>) {
        let mut blockstore_path = options.path.clone();
        let mut datastore_path = options.path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        let block_store = TRepoTypes::TBlockStore::new(blockstore_path);
        let data_store = TRepoTypes::TDataStore::new(datastore_path);
        let (sender, receiver) = crossbeam::unbounded();
        (
            Repo {
                block_store,
                data_store,
                events: sender,
                subscriptions: Default::default(),
            },
            receiver,
        )
    }

    pub async fn init(&self) -> Result<(), Error> {
        let f1 = self.block_store.init();
        let f2 = self.data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    pub async fn open(&self) -> Result<(), Error> {
        let f1 = self.block_store.open();
        let f2 = self.data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    /// Puts a block into the block store.
    pub async fn put_block(&self, block: Block) -> Result<Cid, Error> {
        let cid = self.block_store.put(block.clone()).await?;
        if let Some(subscription) = self.subscriptions.lock().unwrap().remove(&cid) {
            subscription.lock().unwrap().wake(block);
        }
        // sending only fails if no one is listening anymore
        // and that is okay with us.
        self.events.send(RepoEvent::ProvideBlock(cid.clone())).ok();
        Ok(cid)
    }

    /// Retrives a block from the block store.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        let subscription = if let Some(block) = self.block_store.get(&cid.clone()).await? {
            Arc::new(Mutex::new(Subscription::ready(block)))
        } else {
            // sending only fails if no one is listening anymore
            // and that is okay with us.
            self.events.send(RepoEvent::WantBlock(cid.clone())).ok();
            self.subscriptions
                .lock()
                .unwrap()
                .entry(cid.clone())
                .or_default()
                .clone()
        };
        Ok(BlockFuture { subscription }.await)
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid) -> Result<(), Error> {
        // sending only fails if no one is listening anymore
        // and that is okay with us.
        self.events
            .send(RepoEvent::UnprovideBlock(cid.to_owned()))
            .ok();
        self.block_store.remove(cid).await?;
        Ok(())
    }

    /// Get an ipld path from the datastore.
    pub async fn get_ipns(&self, ipns: &PeerId) -> Result<Option<IpfsPath>, Error> {
        use std::str::FromStr;

        let data_store = self.data_store.clone();
        let key = ipns.to_owned();
        let bytes = data_store.get(Column::Ipns, key.as_bytes()).await?;
        match bytes {
            Some(ref bytes) => {
                let string = String::from_utf8_lossy(bytes);
                let path = IpfsPath::from_str(&string)?;
                Ok(Some(path))
            }
            None => Ok(None),
        }
    }

    /// Put an ipld path into the datastore.
    pub async fn put_ipns(&self, ipns: &PeerId, path: &IpfsPath) -> Result<(), Error> {
        let string = path.to_string();
        let value = string.as_bytes();
        self.data_store
            .put(Column::Ipns, ipns.as_bytes(), value)
            .await
    }

    /// Remove an ipld path from the datastore.
    pub async fn remove_ipns(&self, ipns: &PeerId) -> Result<(), Error> {
        self.data_store.remove(Column::Ipns, ipns.as_bytes()).await
    }
}

#[async_trait]
impl<T: RepoTypes> bitswap::BitswapStore for Repo<T> {
    async fn get_block(&self, cid: &Cid) -> Result<Option<Block>, anyhow::Error> {
        self.block_store.get(cid).await
    }

    async fn put_block(&self, block: Block) -> Result<(), anyhow::Error> {
        self.put_block(block).await?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Subscription {
    block: Option<Block>,
    wakers: Vec<Waker>,
}

impl Subscription {
    pub fn ready(block: Block) -> Self {
        Self {
            block: Some(block),
            wakers: vec![],
        }
    }

    pub fn add_waker(&mut self, waker: Waker) {
        self.wakers.push(waker);
    }

    pub fn result(&self) -> Option<Block> {
        self.block.clone()
    }

    pub fn wake(&mut self, block: Block) {
        self.block = Some(block);
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

pub struct BlockFuture {
    subscription: Arc<Mutex<Subscription>>,
}

impl Future for BlockFuture {
    type Output = Block;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscription = self.subscription.lock().unwrap();
        if let Some(result) = subscription.result() {
            Poll::Ready(result)
        } else {
            subscription.add_waker(context.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::env::temp_dir;

    #[derive(Clone)]
    pub struct Types;

    impl RepoTypes for Types {
        type TBlockStore = mem::MemBlockStore;
        type TDataStore = mem::MemDataStore;
    }

    pub fn create_mock_repo() -> (Arc<Repo<Types>>, Receiver<RepoEvent>) {
        let mut tmp = temp_dir();
        tmp.push("rust-ipfs-repo");
        let options: RepoOptions<Types> = RepoOptions {
            _marker: PhantomData,
            path: tmp.into(),
        };
        let (r, ch) = Repo::new(options);
        (Arc::new(r), ch)
    }

    #[async_std::test]
    async fn test_repo() {
        let (repo, _) = create_mock_repo();
        repo.init().await.unwrap();
    }
}
