//! IPFS repo
use crate::block::{Cid, Block};
use crate::config::ConfigFile;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub trait RepoTypes {
    type TBlockStore: BlockStore = MemBlockStore;
    type TDataStore: DataStore = MemDataStore;
    type TRepo: Repo<Self::TBlockStore, Self::TDataStore> = IpfsRepo<Self::TBlockStore, Self::TDataStore>;
}

#[derive(Clone, Debug, Default)]
pub struct RepoOptions<TRepoTypes: RepoTypes> {
    _marker: PhantomData<TRepoTypes>,
}

impl<TRepoTypes: RepoTypes> From<&ConfigFile> for RepoOptions<TRepoTypes> {
    fn from(_config: &ConfigFile) -> Self {
        RepoOptions {
            _marker: PhantomData,
        }
    }
}

pub fn create_repo<TRepoTypes: RepoTypes>(_options: RepoOptions<TRepoTypes>) -> TRepoTypes::TRepo {
    TRepoTypes::TRepo::new(TRepoTypes::TBlockStore::new(), TRepoTypes::TDataStore::new())
}


pub trait BlockStore: Clone + Send {
    fn new() -> Self;
    fn contains(&self, cid: &Cid) -> bool;
    fn get(&self, cid: &Cid) -> Option<Block>;
    fn put(&self, block: Block) -> Cid;
    fn remove(&self, cid: &Cid);
}

pub trait DataStore: Clone + Send {
    fn new() -> Self;
}

pub trait Repo<BS: BlockStore, DS: DataStore>: Clone + Send {
    fn new(block_store: BS, data_store: DS) -> Self;
    fn init(&mut self);
    fn open(&mut self);
    fn close(&mut self);
    fn exists(&self) -> bool;
    fn blocks(&self) -> &BS;
    fn data(&self) -> &DS;
}

#[derive(Clone, Debug)]
pub struct IpfsRepo<BS: BlockStore, DS: DataStore> {
    block_store: BS,
    data_store: DS,
}

impl<BS: BlockStore, DS: DataStore> Repo<BS, DS> for IpfsRepo<BS, DS> {
    fn new(block_store: BS, data_store: DS) -> Self {
        IpfsRepo {
            block_store,
            data_store,
        }
    }

    fn init(&mut self) {

    }

    fn open(&mut self) {

    }

    fn close(&mut self) {

    }

    fn exists(&self) -> bool {
        false
    }

    fn blocks(&self) -> &BS {
        &self.block_store
    }

    fn data(&self) -> &DS {
        &self.data_store
    }
}

#[derive(Clone, Debug)]
pub struct MemBlockStore {
    blocks: Arc<Mutex<HashMap<Cid, Block>>>,
}

impl BlockStore for MemBlockStore {
    fn new() -> Self {
        MemBlockStore {
            blocks: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    fn contains(&self, cid: &Cid) -> bool {
        self.blocks.lock().unwrap().contains_key(cid)
    }

    fn get(&self, cid: &Cid) -> Option<Block> {
        self.blocks.lock().unwrap()
            .get(cid)
            .map(|block| block.to_owned())
    }

    fn put(&self, block: Block) -> Cid {
        let cid = block.cid();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        cid
    }

    fn remove(&self, cid: &Cid) {
        self.blocks.lock().unwrap().remove(cid);
    }
}

#[derive(Clone, Debug)]
pub struct MemDataStore {

}

impl DataStore for MemDataStore {
    fn new() -> Self {
        MemDataStore {}
    }
}
