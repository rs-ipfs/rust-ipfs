//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore, Column, DataStore};
use async_std::path::PathBuf;
use async_std::sync::Mutex;
use async_trait::async_trait;
use bitswap::Block;
use libipld::cid::Cid;

use super::{BlockRm, BlockRmError};

// FIXME: Transition to Persistent Map to make iterating more consistent
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MemBlockStore {
    blocks: Mutex<HashMap<Cid, Block>>,
}

#[async_trait]
impl BlockStore for MemBlockStore {
    fn new(_path: PathBuf) -> Self {
        Default::default()
    }

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let contains = self.blocks.lock().await.contains_key(cid);
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let block = self
            .blocks
            .lock()
            .await
            .get(cid)
            .map(|block| block.to_owned());
        Ok(block)
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;
        let mut g = self.blocks.lock().await;
        match g.entry(block.cid.clone()) {
            Entry::Occupied(_) => Ok((block.cid, BlockPut::Existed)),
            Entry::Vacant(ve) => {
                let cid = ve.key().clone();
                ve.insert(block);
                Ok((cid, BlockPut::NewBlock))
            }
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        match self.blocks.lock().await.remove(cid) {
            Some(_block) => Ok(Ok(BlockRm::Removed(cid.clone()))),
            None => Ok(Err(BlockRmError::NotFound(cid.clone()))),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let guard = self.blocks.lock().await;
        Ok(guard.iter().map(|(cid, _block)| cid).cloned().collect())
    }
}

#[derive(Debug, Default)]
pub struct MemDataStore {
    ipns: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    pin: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl DataStore for MemDataStore {
    fn new(_path: PathBuf) -> Self {
        Default::default()
    }

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
            Column::Pin => &self.pin,
        };
        let contains = map.lock().await.contains_key(key);
        Ok(contains)
    }

    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
            Column::Pin => &self.pin,
        };
        let value = map.lock().await.get(key).map(|value| value.to_owned());
        Ok(value)
    }

    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
            Column::Pin => &self.pin,
        };
        map.lock().await.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
            Column::Pin => &self.pin,
        };
        map.lock().await.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitswap::Block;
    use libipld::cid::{Cid, Codec};
    use multihash::Sha2_256;
    use std::env::temp_dir;

    #[async_std::test]
    async fn test_mem_blockstore() {
        let tmp = temp_dir();
        let store = MemBlockStore::new(tmp.into());
        let data = b"1".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid.clone());

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
        if store.remove(&cid).await.unwrap().is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone());
        assert_eq!(put.await.unwrap().0, cid.to_owned());
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap().unwrap();
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
    }

    #[async_std::test]
    async fn test_mem_blockstore_list() {
        let tmp = temp_dir();
        let mem_store = MemBlockStore::new(tmp.into());

        mem_store.init().await.unwrap();
        mem_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec().into_boxed_slice();
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data_slice));
            let block = Block::new(data_slice, cid);
            mem_store.put(block.clone()).await.unwrap();
            assert!(mem_store.contains(block.cid()).await.unwrap());
        }

        let cids = mem_store.list().await.unwrap();
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(mem_store.contains(cid).await.unwrap());
        }
    }

    #[async_std::test]
    async fn test_mem_datastore() {
        let tmp = temp_dir();
        let store = MemDataStore::new(tmp.into());
        let col = Column::Ipns;
        let key = [1, 2, 3, 4];
        let value = [5, 6, 7, 8];

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), None);
        store.remove(col, &key).await.unwrap();

        let put = store.put(col, &key, &value);
        put.await.unwrap();
        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), Some(value.to_vec()));

        store.remove(col, &key).await.unwrap();
        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), None);
    }
}
