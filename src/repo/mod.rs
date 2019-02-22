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
    Repo::new(
        TRepoTypes::TBlockStore::new(options.path.clone()),
        TRepoTypes::TDataStore::new(options.path),
    )
}

pub trait BlockStore: Clone + Send + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }
    fn contains(&self, cid: Cid) -> FutureObj<'static, bool>;
    fn get(&self, cid: Cid) -> FutureObj<'static, Option<Block>>;
    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, std::io::Error>>;
    fn remove(&self, cid: Cid) -> FutureObj<'static, ()>;
}

pub trait DataStore: Clone + Send + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }
}

#[derive(Clone, Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    pub block_store: TRepoTypes::TBlockStore,
    pub data_store: TRepoTypes::TDataStore,
}

impl<TRepoTypes: RepoTypes> Repo<TRepoTypes> {
    pub fn new(block_store: TRepoTypes::TBlockStore, data_store: TRepoTypes::TDataStore) -> Self {
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
}
