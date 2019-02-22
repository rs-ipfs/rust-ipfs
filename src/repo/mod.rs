//! IPFS repo
use crate::block::{Cid, Block};
use crate::IpfsOptions;
use futures::future::FutureObj;
use futures::join;
use std::marker::PhantomData;
use std::path::PathBuf;

pub mod mem;
pub mod fs;

pub trait RepoTypes: Clone + Send + 'static {
    type TBlockStore: BlockStore;
    type TDataStore: DataStore;
}

#[derive(Clone, Debug)]
pub struct RepoOptions<TRepoTypes: RepoTypes> {
    _marker: PhantomData<TRepoTypes>,
    path: PathBuf,
}

impl<TRepoTypes: RepoTypes> From<&IpfsOptions> for RepoOptions<TRepoTypes> {
    fn from(options: &IpfsOptions) -> Self {
        RepoOptions {
            _marker: PhantomData,
            path: options.ipfs_path.clone(),
        }
    }
}

pub fn create_repo<TRepoTypes: RepoTypes>(options: RepoOptions<TRepoTypes>) -> Repo<TRepoTypes> {
    Repo::new(options)
}

pub trait BlockStore: Clone + Send + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>>;
    fn open(&self) -> FutureObj<'static, Result<(), std::io::Error>>;
    fn contains(&self, cid: Cid) -> FutureObj<'static, Result<bool, std::io::Error>>;
    fn get(&self, cid: Cid) -> FutureObj<'static, Result<Option<Block>, std::io::Error>>;
    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, std::io::Error>>;
    fn remove(&self, cid: Cid) -> FutureObj<'static, Result<(), std::io::Error>>;
}

pub trait DataStore: Clone + Send + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>>;
}

#[derive(Clone, Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    pub block_store: TRepoTypes::TBlockStore,
    pub data_store: TRepoTypes::TDataStore,
}

impl<TRepoTypes: RepoTypes> Repo<TRepoTypes> {
    pub fn new(options: RepoOptions<TRepoTypes>) -> Self {
        let mut blockstore_path = options.path.clone();
        let mut datastore_path = options.path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        let block_store = TRepoTypes::TBlockStore::new(blockstore_path);
        let data_store = TRepoTypes::TDataStore::new(datastore_path);
        Repo {
            block_store,
            data_store,
        }
    }

    pub fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        let block_store = self.block_store.clone();
        let data_store = self.data_store.clone();
        FutureObj::new(Box::new(async move {
            let f1 = block_store.init();
            let f2 = data_store.init();
            let (r1, r2) = join!(f1, f2);
            if r1.is_err() {
                r1
            } else {
                r2
            }
        }))
    }

    pub fn open(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        self.block_store.open()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;
    use std::env::temp_dir;

    #[derive(Clone)]
    struct Types;

    impl RepoTypes for Types {
        type TBlockStore = mem::MemBlockStore;
        type TDataStore = mem::MemDataStore;
    }

    #[test]
    fn test_repo() {
        let mut tmp = temp_dir();
        tmp.push("rust-ipfs-repo");
        let options: RepoOptions<Types> = RepoOptions {
            _marker: PhantomData,
            path: tmp,
        };
        let repo = Repo::new(options);
        tokio::run(FutureObj::new(Box::new(async move {
            await!(repo.init()).unwrap();
            Ok(())
        })).compat());
    }
}
