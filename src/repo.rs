//! IPFS repo
use crate::block::{Cid, Block};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Repo {
    blocks: Arc<Mutex<HashMap<Cid, Block>>>,
}

impl Repo {
    pub fn new() -> Self {
        Repo {
            blocks: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn contains(&self, cid: &Cid) -> bool {
        self.blocks.lock().unwrap().contains_key(cid)
    }

    pub fn get(&self, cid: &Cid) -> Option<Block> {
        self.blocks.lock().unwrap()
            .get(cid)
            .map(|block| (*block).clone())
    }

    pub fn put(&self, block: Block) -> Cid {
        let cid = block.cid();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        cid
    }
}
