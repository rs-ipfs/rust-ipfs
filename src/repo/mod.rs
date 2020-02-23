//! IPFS repo
use crate::block::{Block, Cid};
use crate::error::Error;
use crate::path::IpfsPath;
use crate::IpfsOptions;
use async_std::path::PathBuf;
use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::SinkExt;
use libp2p::PeerId;
use std::marker::PhantomData;

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
) -> (Repo<TRepoTypes>, Receiver<RepoEvent>) {
    Repo::new(options)
}

#[async_trait]
pub trait BlockStore: Clone + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    async fn put(&self, block: Block) -> Result<Cid, Error>;
    async fn remove(&self, cid: &Cid) -> Result<(), Error>;
}

#[async_trait]
pub trait DataStore: Clone + Send + Sync + Unpin + 'static {
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

#[derive(Clone, Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    block_store: TRepoTypes::TBlockStore,
    data_store: TRepoTypes::TDataStore,
    events: Sender<RepoEvent>,
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
        let (sender, receiver) = channel::<RepoEvent>(1);
        (
            Repo {
                block_store,
                data_store,
                events: sender,
            },
            receiver,
        )
    }

    pub async fn init(&self) -> Result<(), Error> {
        let block_store = self.block_store.clone();
        let data_store = self.data_store.clone();
        let f1 = block_store.init();
        let f2 = data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    pub async fn open(&self) -> Result<(), Error> {
        let block_store = self.block_store.clone();
        let data_store = self.data_store.clone();
        let f1 = block_store.open();
        let f2 = data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    /// Puts a block into the block store.
    pub async fn put_block(&mut self, block: Block) -> Result<Cid, Error> {
        let cid = self.block_store.put(block).await?;
        // sending only fails if no one is listening anymore
        // and that is okay with us.
        let _ = self.events.send(RepoEvent::ProvideBlock(cid.clone())).await;
        Ok(cid)
    }

    /// Retrives a block from the block store.
    pub async fn get_block(&mut self, cid: &Cid) -> Result<Block, Error> {
        loop {
            if !self.block_store.contains(&cid).await? {
                // sending only fails if no one is listening anymore
                // and that is okay with us.
                let _ = self.events.send(RepoEvent::WantBlock(cid.clone())).await;
            }
            match self.block_store.get(&cid.clone()).await? {
                Some(block) => return Ok(block),
                None => continue,
            }
        }
    }

    /// Remove block from the block store.
    pub async fn remove_block(&mut self, cid: &Cid) -> Result<(), Error> {
        // sending only fails if no one is listening anymore
        // and that is okay with us.
        let _ = self
            .events
            .send(RepoEvent::UnprovideBlock(cid.to_owned()))
            .await;
        self.block_store.remove(cid).await?;
        Ok(())
    }

    /// Get an ipld path from the datastore.
    pub async fn get_ipns(&mut self, ipns: &PeerId) -> Result<Option<IpfsPath>, Error> {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::tests::async_test;
    use std::env::temp_dir;

    #[derive(Clone)]
    pub struct Types;

    impl RepoTypes for Types {
        type TBlockStore = mem::MemBlockStore;
        type TDataStore = mem::MemDataStore;
    }

    pub fn create_mock_repo() -> Repo<Types> {
        let mut tmp = temp_dir();
        tmp.push("rust-ipfs-repo");
        let options: RepoOptions<Types> = RepoOptions {
            _marker: PhantomData,
            path: tmp.into(),
        };
        let (r, _) = Repo::new(options);
        r
    }

    #[test]
    fn test_repo() {
        let repo = create_mock_repo();
        async_test(async move {
            repo.init().await.unwrap();
        });
    }
}
