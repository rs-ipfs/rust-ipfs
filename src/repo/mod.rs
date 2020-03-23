//! IPFS repo
use crate::daemon::IpfsEvent;
use crate::error::Error;
use crate::options::{IpfsOptions, IpfsTypes};
use crate::path::IpfsPath;
use crate::subscription::SubscriptionRegistry;
use async_std::path::PathBuf;
use async_std::sync::Mutex;
use async_trait::async_trait;
use bitswap::Block;
use core::fmt::Debug;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use libipld::cid::Cid;
use libp2p::core::PeerId;

pub mod fs;
pub mod mem;

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
pub struct Repo<T: IpfsTypes> {
    block_store: T::TBlockStore,
    data_store: T::TDataStore,
    sender: Sender<IpfsEvent>,
    subscriptions: Mutex<SubscriptionRegistry<Cid, Block>>,
}

impl<T: IpfsTypes> Repo<T> {
    pub fn new(options: &IpfsOptions, sender: Sender<IpfsEvent>) -> Self {
        let blockstore_path = options.ipfs_path.join("blockstore");
        let datastore_path = options.ipfs_path.join("datastore");
        let block_store = T::TBlockStore::new(blockstore_path);
        let data_store = T::TDataStore::new(datastore_path);
        Repo {
            block_store,
            data_store,
            sender,
            subscriptions: Default::default(),
        }
    }

    pub async fn init(&self) -> Result<(), Error> {
        let f1 = self.block_store.init();
        let f2 = self.data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        r1?;
        r2?;
        Ok(())
    }

    pub async fn open(&self) -> Result<(), Error> {
        let f1 = self.block_store.open();
        let f2 = self.data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        r1?;
        r2?;
        Ok(())
    }

    /// Puts a block into the block store.
    pub async fn put_block(&self, block: Block) -> Result<Cid, Error> {
        let cid = self.block_store.put(block.clone()).await?;
        self.subscriptions
            .lock()
            .await
            .finish_subscription(&cid, block);
        // sending only fails if no one is listening anymore
        // and that is okay with us.
        self.sender
            .clone()
            .send(IpfsEvent::ProvideBlock(cid.clone()))
            .await
            .ok();
        Ok(cid)
    }

    /// Retrives a block from the block store.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        if let Some(block) = self.block_store.get(cid).await? {
            Ok(block)
        } else {
            let subscription = self
                .subscriptions
                .lock()
                .await
                .create_subscription(cid.clone());
            // sending only fails if no one is listening anymore
            // and that is okay with us.
            self.sender
                .clone()
                .send(IpfsEvent::WantBlock(cid.clone()))
                .await
                .ok();
            Ok(subscription.await)
        }
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid) -> Result<(), Error> {
        // sending only fails if the background task has exited
        self.sender
            .clone()
            .send(IpfsEvent::UnprovideBlock(cid.clone()))
            .await
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
impl<T: IpfsTypes> bitswap::BitswapStore for Repo<T> {
    async fn get_block(&self, cid: &Cid) -> Result<Option<Block>, anyhow::Error> {
        self.block_store.get(cid).await
    }

    async fn put_block(&self, block: Block) -> Result<(), anyhow::Error> {
        self.put_block(block).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::options::TestTypes;
    use futures::channel::mpsc::{channel, Receiver};
    use std::sync::Arc;

    pub fn create_mock_repo() -> (Arc<Repo<TestTypes>>, Receiver<IpfsEvent>) {
        let options = IpfsOptions::inmemory_with_generated_keys(false);
        let (tx, rx) = channel(1);
        let repo = Arc::new(Repo::<TestTypes>::new(&options, tx));
        (repo, rx)
    }

    #[async_std::test]
    async fn test_repo() {
        let (repo, _) = create_mock_repo();
        repo.init().await.unwrap();
    }
}
