//! Volatile memory backed repo
use crate::block::{Cid, Block};
use crate::error::Error;
use crate::repo::{BlockStore, DataStore, Column};
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

    fn init(&self) -> FutureObj<'static, Result<(), Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn open(&self) -> FutureObj<'static, Result<(), Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn contains(&self, cid: &Cid) -> FutureObj<'static, Result<bool, Error>> {
        let contains = self.blocks.lock().unwrap().contains_key(cid);
        FutureObj::new(Box::new(futures::future::ok(contains)))
    }

    fn get(&self, cid: &Cid) -> FutureObj<'static, Result<Option<Block>, Error>> {
        let block = self.blocks.lock().unwrap()
            .get(cid)
            .map(|block| block.to_owned());
        FutureObj::new(Box::new(futures::future::ok(block)))
    }

    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, Error>> {
        let cid = block.cid().to_owned();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        FutureObj::new(Box::new(futures::future::ok(cid)))
    }

    fn remove(&self, cid: &Cid) -> FutureObj<'static, Result<(), Error>> {
        self.blocks.lock().unwrap().remove(cid);
        FutureObj::new(Box::new(futures::future::ok(())))
    }
}

#[derive(Clone, Debug)]
pub struct MemDataStore {
    ipns: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl DataStore for MemDataStore {
    fn new(_path: PathBuf) -> Self {
        MemDataStore {
            ipns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn init(&self) -> FutureObj<'static, Result<(), Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn open(&self) -> FutureObj<'static, Result<(), Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn contains(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<bool, Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        let contains = map.lock().unwrap().contains_key(key);
        FutureObj::new(Box::new(futures::future::ok(contains)))
    }

    fn get(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<Option<Vec<u8>>, Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        let value = map.lock().unwrap().get(key).map(|value| value.to_owned());
        FutureObj::new(Box::new(futures::future::ok(value)))
    }

    fn put(&self, col: Column, key: &[u8], value: &[u8]) ->
        FutureObj<'static, Result<(), Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        map.lock().unwrap().insert(key.to_owned(), value.to_owned());
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn remove(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<(), Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        map.lock().unwrap().remove(key);
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
        let tmp = temp_dir();
        let store = MemBlockStore::new(tmp);
        tokio_run(async move {
            let block = Block::from("1");
            let cid = block.cid();

            assert_eq!(await!(store.init()).unwrap(), ());
            assert_eq!(await!(store.open()).unwrap(), ());

            let contains = store.contains(cid);
            assert_eq!(await!(contains).unwrap(), false);
            let get = store.get(cid);
            assert_eq!(await!(get).unwrap(), None);
            let remove = store.remove(cid);
            assert_eq!(await!(remove).unwrap(), ());

            let put = store.put(block.clone());
            assert_eq!(await!(put).unwrap(), cid.to_owned());
            let contains = store.contains(cid);
            assert_eq!(await!(contains).unwrap(), true);
            let get = store.get(cid);
            assert_eq!(await!(get).unwrap(), Some(block.clone()));

            let remove = store.remove(cid);
            assert_eq!(await!(remove).unwrap(), ());
            let contains = store.contains(cid);
            assert_eq!(await!(contains).unwrap(), false);
            let get = store.get(cid);
            assert_eq!(await!(get).unwrap(), None);
        });
    }

    #[test]
    fn test_mem_datastore() {
        let tmp = temp_dir();
        let store = MemDataStore::new(tmp);
        tokio::run_async(async move {
            let col = Column::Ipns;
            let key = [1, 2, 3, 4];
            let value = [5, 6, 7, 8];

            assert_eq!(await!(store.init()).unwrap(), ());
            assert_eq!(await!(store.open()).unwrap(), ());

            let contains = store.contains(col, &key);
            assert_eq!(await!(contains).unwrap(), false);
            let get = store.get(col, &key);
            assert_eq!(await!(get).unwrap(), None);
            let remove = store.remove(col, &key);
            assert_eq!(await!(remove).unwrap(), ());

            let put = store.put(col, &key, &value);
            assert_eq!(await!(put).unwrap(), ());
            let contains = store.contains(col, &key);
            assert_eq!(await!(contains).unwrap(), true);
            let get = store.get(col, &key);
            assert_eq!(await!(get).unwrap(), Some(value.to_vec()));

            let remove = store.remove(col, &key);
            assert_eq!(await!(remove).unwrap(), ());
            let contains = store.contains(col, &key);
            assert_eq!(await!(contains).unwrap(), false);
            let get = store.get(col, &key);
            assert_eq!(await!(get).unwrap(), None);
        });
    }
}
