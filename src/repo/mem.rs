//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{
    BlockPut, BlockStore, Column, DataStore, PinDocument, PinKind, PinMode, PinStore, Recursive,
};
use async_trait::async_trait;
use bitswap::Block;
use cid::Cid;
use futures::lock::Mutex;
use std::path::PathBuf;

use super::{BlockRm, BlockRmError, RepoCid};

// FIXME: Transition to Persistent Map to make iterating more consistent
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MemBlockStore {
    blocks: Mutex<HashMap<RepoCid, Block>>,
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
        let contains = self
            .blocks
            .lock()
            .await
            .contains_key(&RepoCid(cid.to_owned()));
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let block = self
            .blocks
            .lock()
            .await
            .get(&RepoCid(cid.to_owned()))
            .map(|block| block.to_owned());
        Ok(block)
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;
        let mut g = self.blocks.lock().await;
        match g.entry(RepoCid(block.cid.clone())) {
            Entry::Occupied(_) => {
                trace!("already existing block");
                Ok((block.cid, BlockPut::Existed))
            }
            Entry::Vacant(ve) => {
                trace!("new block");
                let cid = ve.key().0.clone();
                ve.insert(block);
                Ok((cid, BlockPut::NewBlock))
            }
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        match self.blocks.lock().await.remove(&RepoCid(cid.to_owned())) {
            Some(_block) => Ok(Ok(BlockRm::Removed(cid.clone()))),
            None => Ok(Err(BlockRmError::NotFound(cid.clone()))),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let guard = self.blocks.lock().await;
        Ok(guard.iter().map(|(cid, _block)| cid.0.clone()).collect())
    }

    async fn wipe(&self) {
        self.blocks.lock().await.clear();
    }
}

#[derive(Debug, Default)]
pub struct MemDataStore {
    ipns: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    pin: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl PinStore for MemDataStore {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        let g = self.pin.lock().await;
        let key = block.to_bytes();
        Ok(g.contains_key(&key))
    }

    async fn insert_pin(&self, target: &Cid, kind: PinKind<&'_ Cid>) -> Result<(), Error> {
        use std::collections::hash_map::Entry;
        let mut g = self.pin.lock().await;

        // rationale for storing as Cid: the same multihash can be pinned with different codecs.
        // even if there aren't many polyglot documents known, pair of raw and the actual codec is
        // always a possibility.
        let key = target.to_bytes();

        match g.entry(key) {
            Entry::Occupied(mut oe) => {
                let mut doc: PinDocument = serde_json::from_slice(oe.get())?;
                if doc.update(true, &kind)? {
                    let vec = oe.get_mut();
                    vec.clear();
                    serde_json::to_writer(vec, &doc)?;
                    trace!(doc = ?doc, kind = ?kind, "updated on insert");
                } else {
                    trace!(doc = ?doc, kind = ?kind, "update not needed on insert");
                }
            }
            Entry::Vacant(ve) => {
                let mut doc = PinDocument {
                    version: 0,
                    direct: false,
                    recursive: Recursive::Not,
                    cid_version: match target.version() {
                        cid::Version::V0 => 0,
                        cid::Version::V1 => 1,
                    },
                    indirect_by: Vec::new(),
                };

                doc.update(true, &kind).unwrap();
                let vec = serde_json::to_vec(&doc)?;
                ve.insert(vec);
                trace!(doc = ?doc, kind = ?kind, "created on insert");
            }
        }

        Ok(())
    }

    async fn remove_pin(&self, target: &Cid, kind: PinKind<&'_ Cid>) -> Result<(), Error> {
        use std::collections::hash_map::Entry;

        let mut g = self.pin.lock().await;

        // see cid vs. multihash from [`insert_pin`]
        let key = target.to_bytes();

        match g.entry(key) {
            Entry::Occupied(mut oe) => {
                let mut doc: PinDocument = serde_json::from_slice(oe.get())?;
                if !doc.update(false, &kind)? {
                    trace!(doc = ?doc, kind = ?kind, "update not needed on removal");
                    return Ok(());
                }

                if doc.can_remove() {
                    oe.remove();
                } else {
                    let vec = oe.get_mut();
                    vec.clear();
                    serde_json::to_writer(vec, &doc)?;
                }

                Ok(())
            }
            Entry::Vacant(_) => Err(anyhow::anyhow!("not pinned")),
        }
    }

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        use futures::stream::StreamExt;
        use std::convert::TryFrom;
        let g = self.pin.lock().await;

        let copy = g
            .iter()
            .map(|(key, value)| {
                let cid = Cid::try_from(key.as_slice())?;
                let doc: PinDocument = serde_json::from_slice(value)?;
                let mode = doc.mode().ok_or_else(|| anyhow::anyhow!("invalid mode"))?;

                Ok((cid, mode))
            })
            .filter(move |res| {
                // could return just two different boxed streams
                if let Some(f) = &mode {
                    match res {
                        Ok((_, mode)) => mode == f,
                        Err(_) => true,
                    }
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();

        futures::stream::iter(copy).boxed()
    }

    async fn query(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let g = self.pin.lock().await;

        cids.into_iter()
            .map(move |cid| {
                match g.get(&cid.to_bytes()) {
                    Some(raw) => {
                        let doc: PinDocument = match serde_json::from_slice(raw) {
                            Ok(doc) => doc,
                            Err(e) => return Err(e.into()),
                        };
                        // None from document is bad result, since the document shouldn't exist in the
                        // first place
                        let mode = match doc.pick_kind() {
                            Some(Ok(kind)) => kind,
                            Some(Err(invalid_cid)) => return Err(Error::new(invalid_cid)),
                            None => {
                                trace!(doc = ?doc, "could not pick pin kind");
                                return Err(anyhow::anyhow!("{} is not pinned", cid));
                            }
                        };

                        // would be more clear if this business was in a separate map; quite awful
                        // as it is now

                        let matches = requirement.as_ref().map(|req| mode == req).unwrap_or(true);

                        if matches {
                            trace!(cid = %cid, req = ?requirement, "pin matches");
                            return Ok((cid, mode));
                        } else {
                            // FIXME: this error is about the same as http api expects
                            return Err(anyhow::anyhow!(
                                "{} is not pinned as {:?}",
                                cid,
                                requirement.expect("matches is never false if requirement is none")
                            ));
                        }
                    }
                    None => {
                        trace!(cid = %cid, "no record found");
                    }
                }

                // FIXME: this error is expected on http interface
                Err(anyhow::anyhow!("{} is not pinned", cid))
            })
            .collect::<Result<Vec<_>, _>>()
    }
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

    async fn wipe(&self) {
        self.ipns.lock().await.clear();
        self.pin.lock().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitswap::Block;
    use cid::{Cid, Codec};
    use multihash::Sha2_256;
    use std::env::temp_dir;

    #[tokio::test(max_threads = 1)]
    async fn test_mem_blockstore() {
        let tmp = temp_dir();
        let store = MemBlockStore::new(tmp);
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

    #[tokio::test(max_threads = 1)]
    async fn test_mem_blockstore_list() {
        let tmp = temp_dir();
        let mem_store = MemBlockStore::new(tmp);

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

    #[tokio::test(max_threads = 1)]
    async fn test_mem_datastore() {
        let tmp = temp_dir();
        let store = MemDataStore::new(tmp);
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
