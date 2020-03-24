//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{BlockStore, BlockStoreEvent, Column, DataStore};
use async_std::path::PathBuf;
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use futures::Stream;
use libipld::cid::Cid;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Default)]
pub struct MemBlockStore {
    blocks: HashMap<Cid, Box<[u8]>>,
    events: VecDeque<BlockStoreEvent>,
    waker: Option<Waker>,
}

#[async_trait]
impl BlockStore for MemBlockStore {
    async fn open(_path: PathBuf) -> Result<Self, Error> {
        Ok(Self::default())
    }

    fn contains(&mut self, cid: &Cid) -> bool {
        self.blocks.contains_key(cid)
    }

    fn get(&mut self, cid: Cid) {
        let block = self.blocks.get(&cid).cloned();
        self.events.push_back(BlockStoreEvent::Get(cid, Ok(block)));
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }

    fn put(&mut self, cid: Cid, data: Box<[u8]>) {
        self.blocks.insert(cid.clone(), data);
        self.events.push_back(BlockStoreEvent::Put(cid, Ok(())));
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }

    fn remove(&mut self, cid: Cid) {
        self.blocks.remove(&cid);
        self.events.push_back(BlockStoreEvent::Remove(cid, Ok(())));
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl Stream for MemBlockStore {
    type Item = BlockStoreEvent;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(Some(event))
        } else {
            self.waker = Some(ctx.waker().clone());
            Poll::Pending
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemDataStore {
    ipns: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

#[async_trait]
impl DataStore for MemDataStore {
    fn new(_path: PathBuf) -> Self {
        MemDataStore {
            ipns: Arc::new(Mutex::new(HashMap::new())),
        }
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
        };
        let contains = map.lock().await.contains_key(key);
        Ok(contains)
    }

    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        let value = map.lock().await.get(key).map(|value| value.to_owned());
        Ok(value)
    }

    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        map.lock().await.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
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

        assert_eq!(store.init().await.unwrap(), ());
        assert_eq!(store.open().await.unwrap(), ());

        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
        let remove = store.remove(&cid);
        assert_eq!(remove.await.unwrap(), ());

        let put = store.put(block.clone());
        assert_eq!(put.await.unwrap(), cid.to_owned());
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        let remove = store.remove(&cid);
        assert_eq!(remove.await.unwrap(), ());
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
    }

    #[async_std::test]
    async fn test_mem_datastore() {
        let tmp = temp_dir();
        let store = MemDataStore::new(tmp.into());
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
    }
}
