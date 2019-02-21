//! Volatile memory backed repo
use crate::block::{Cid, Block};
use crate::repo::{BlockStore, DataStore};
use futures::future::FutureObj;
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

    fn contains(&self, cid: Cid) -> FutureObj<'static, bool> {
        let contains = self.blocks.lock().unwrap().contains_key(&cid);
        FutureObj::new(Box::new(futures::future::ready(contains)))
    }

    fn get(&self, cid: Cid) -> FutureObj<'static, Option<Block>> {
        let block = self.blocks.lock().unwrap()
            .get(&cid)
            .map(|block| block.to_owned());
        FutureObj::new(Box::new(futures::future::ready(block)))
    }

    fn put(&self, block: Block) -> FutureObj<'static, Cid> {
        let cid = block.cid();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        FutureObj::new(Box::new(futures::future::ready(cid)))
    }

    fn remove(&self, cid: Cid) -> FutureObj<'static, ()> {
        self.blocks.lock().unwrap().remove(&cid);
        FutureObj::new(Box::new(futures::future::ready(())))
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
