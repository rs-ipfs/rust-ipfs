//! Persistent fs backed repo
use crate::block::{Cid, Block};
use crate::repo::{BlockStore, DataStore};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct FsBlockStore {
    path: PathBuf,
}

impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
        }
    }

    fn contains(&self, _cid: &Cid) -> bool {
        false
    }

    fn get(&self, _cid: &Cid) -> Option<Block> {
        None
    }

    fn put(&self, block: Block) -> Cid {
        block.cid()
    }

    fn remove(&self, _cid: &Cid) {
    }
}

#[derive(Clone, Debug)]
pub struct RocksDataStore {
    path: PathBuf,
}

impl DataStore for RocksDataStore {
    fn new(path: PathBuf) -> Self {
        RocksDataStore {
            path
        }
    }
}
