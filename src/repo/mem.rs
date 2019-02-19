//! Volatile memory backed repo
use crate::block::{Cid, Block};
use crate::repo::{BlockStore, DataStore};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct MemBlockStore {
    blocks: Arc<Mutex<HashMap<Cid, Block>>>,
}

impl BlockStore for MemBlockStore {
    fn new(_path: PathBuf) -> Self {
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
    fn new(_path: PathBuf) -> Self {
        MemDataStore {}
    }
}
