//! Volatile memory backed repo
use crate::block::{Cid, Block};
use crate::error::Error;
use crate::repo::{BlockStore, DataStore, Column};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::pin::Pin;

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

    fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    fn contains(&self, cid: &Cid) -> Pin<Box<(dyn Future<Output = Result<bool, failure::Error>>)>> {
        Box::pin({
            let contains = self.blocks.lock().unwrap().contains_key(cid);
            Ok(contains)
        })
    }

    fn get(&self, cid: &Cid) -> Pin<Box<dyn Future<Output = Result<Option<Block>, failure::Error>>>> {
        Box::pin({
            let block = self.blocks.lock().unwrap()
                .get(cid)
                .map(|block| block.to_owned());
            Ok(block)
        })
    }

    fn put(&self, block: Block) -> dyn Future<Output = Result<Cid, Error>> {
        let cid = block.cid().to_owned();
        self.blocks.lock().unwrap()
            .insert(cid.clone(), block);
        Ok(cid)
    }

    fn remove(&self, cid: &Cid) -> dyn Future<Output = Result<(), Error>> {
        self.blocks.lock().unwrap().remove(cid);
        Ok(())
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

    fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    fn contains(&self, col: Column, key: &[u8]) ->
        Pin<Box<(dyn Future<Output = Result<bool, Error>> + Send + 'static)>>
    {
        Box::pin({
            let map = match col {
                Column::Ipns => &self.ipns
            };
            let contains = map.lock().unwrap().contains_key(key);
            Ok(contains)
        })
    }

    fn get(&self, col: Column, key: &[u8]) ->
        Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, Error>>>>
    {
        Box::pin({
            let map = match col {
                Column::Ipns => &self.ipns
            };
            let value = map.lock().unwrap().get(key).map(|value| value.to_owned());
            Ok(value)
        })
    }

    fn put(&self, col: Column, key: &[u8], value: &[u8]) ->
        dyn Future<Output = Result<(), Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        map.lock().unwrap().insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn remove(&self, col: Column, key: &[u8]) ->
        dyn Future<Output = Result<(), Error>>
    {
        let map = match col {
            Column::Ipns => &self.ipns
        };
        map.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    #[test]
    fn test_mem_blockstore() {
        let tmp = temp_dir();
        let store = MemBlockStore::new(tmp);
        tokio::run_async(async move {
            let block = Block::from("1");
            let cid = block.cid();

            assert_eq!(store.init().await.unwrap(), ());
            assert_eq!(store.open().await.unwrap(), ());

            let contains = store.contains(cid);
            assert_eq!(contains.await.unwrap(), false);
            let get = store.get(cid);
            assert_eq!(get.await.unwrap(), None);
            let remove = store.remove(cid);
            assert_eq!(remove.await.unwrap(), ());

            let put = store.put(block.clone());
            assert_eq!(put.await.unwrap(), cid.to_owned());
            let contains = store.contains(cid);
            assert_eq!(contains.await.unwrap(), true);
            let get = store.get(cid);
            assert_eq!(get.await.unwrap(), Some(block.clone()));

            let remove = store.remove(cid);
            assert_eq!(remove.await.unwrap(), ());
            let contains = store.contains(cid);
            assert_eq!(contains.await.unwrap(), false);
            let get = store.get(cid);
            assert_eq!(get.await.unwrap(), None);
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

            assert_eq!(store.init().await.unwrap(), ());
            assert_eq!(store.open().await.unwrap(), ());

            let contains = store.contains(col, &key);
            assert_eq!(contains.await.unwrap(), false);
            let get = store.get(col, &key);
            assert_eq!(get.await.unwrap(), None);
            let remove = store.remove(col, &key);
            assert_eq!(remove.await.unwrap(), ());

            let put = store.put(col, &key, &value);
            assert_eq!(put.await.unwrap(), ());
            let contains = store.contains(col, &key);
            assert_eq!(contains.await.unwrap(), true);
            let get = store.get(col, &key);
            assert_eq!(get.await.unwrap(), Some(value.to_vec()));

            let remove = store.remove(col, &key);
            assert_eq!(remove.await.unwrap(), ());
            let contains = store.contains(col, &key);
            assert_eq!(contains.await.unwrap(), false);
            let get = store.get(col, &key);
            assert_eq!(get.await.unwrap(), None);
        });
    }
}
