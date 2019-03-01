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

    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn open(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn contains(&self, cid: &Cid) -> FutureObj<'static, Result<bool, std::io::Error>> {
        let contains = self.blocks.lock().unwrap().contains_key(cid);
        FutureObj::new(Box::new(futures::future::ok(contains)))
    }

    fn get(&self, cid: &Cid) -> FutureObj<'static, Result<Option<Block>, std::io::Error>> {
        let block = self.blocks.lock().unwrap()
            .get(cid)
            .map(|block| block.to_owned());
        FutureObj::new(Box::new(futures::future::ok(block)))
    }

    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, std::io::Error>> {
        let cid = block.cid().to_owned();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        FutureObj::new(Box::new(futures::future::ok(cid)))
    }

    fn remove(&self, cid: &Cid) -> FutureObj<'static, Result<(), std::io::Error>> {
        self.blocks.lock().unwrap().remove(cid);
        FutureObj::new(Box::new(futures::future::ok(())))
    }
}

#[derive(Clone, Debug)]
pub struct MemDataStore {

}

impl DataStore for MemDataStore {
    fn new(_path: PathBuf) -> Self {
        MemDataStore {}
    }

    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use crate::tokio_run;

    #[test]
    fn test_mem_blockstore() {
        let block = Block::from("1");
        let tmp = temp_dir();
        let block_store = MemBlockStore::new(tmp);
        tokio_run(async move {
            await!(block_store.init()).unwrap();
            await!(block_store.open()).unwrap();

            assert!(!await!(block_store.contains(block.cid())).unwrap());
            assert_eq!(await!(block_store.get(block.cid())).unwrap(), None);

            await!(block_store.put(block.clone())).unwrap();
            assert!(await!(block_store.contains(block.cid())).unwrap());
            assert_eq!(await!(block_store.get(block.cid())).unwrap().unwrap(), block);

            await!(block_store.remove(block.cid())).unwrap();
            assert!(!await!(block_store.contains(block.cid())).unwrap());
            assert_eq!(await!(block_store.get(block.cid())).unwrap(), None);
        });
    }
}
