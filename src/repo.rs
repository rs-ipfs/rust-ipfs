//! IPFS repo
use crate::block::Block;
use cid::Cid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Repo {
    blocks: Arc<Mutex<HashMap<Vec<u8>, Block>>>,
}

impl Repo {
    pub fn new() -> Self {
        Repo {
            blocks: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn contains(&self, cid: &Arc<Cid>) -> bool {
        self.blocks.lock().unwrap().contains_key(&cid.hash)
    }

    pub fn get(&self, cid: &Arc<Cid>) -> Option<Block> {
        self.blocks.lock().unwrap()
            .get(&cid.hash)
            .map(|block| (*block).clone())
    }

    pub fn put(&self, block: Block) -> Arc<Cid> {
        let cid = block.cid();
        self.blocks.lock().unwrap()
            .insert(cid.hash.clone(), block);
        cid
    }
}
