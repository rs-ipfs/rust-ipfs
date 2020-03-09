//! Persistent fs backed repo
use crate::block::{Block, Cid};
use crate::error::Error;
use crate::repo::BlockStore;
#[cfg(feature = "rocksdb")]
use crate::repo::{Column, DataStore};
use async_std::fs;
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_trait::async_trait;
use core::convert::TryFrom;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct FsBlockStore {
    path: PathBuf,
    cids: Arc<Mutex<HashSet<Cid>>>,
}

#[async_trait]
impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
            cids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        let path = self.path.clone();
        fs::create_dir_all(path).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        let path = self.path.clone();
        let cids = self.cids.clone();

        let mut stream = fs::read_dir(path).await?;

        fn append_cid(cids: &Arc<Mutex<HashSet<Cid>>>, path: PathBuf) {
            if path.extension() != Some(OsStr::new("data")) {
                return;
            }
            let cid_str = path.file_stem().unwrap();
            let cid = Cid::try_from(cid_str.to_str().unwrap()).unwrap();
            cids.lock().unwrap_or_else(|p| p.into_inner()).insert(cid);
        }

        loop {
            let dir = stream.next().await;

            match dir {
                Some(Ok(dir)) => append_cid(&cids, dir.path()),
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let contains = self.cids.lock().unwrap().contains(cid);
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let path = block_path(self.path.clone(), cid);
        let cid = cid.to_owned();
        let mut file = match fs::File::open(path).await {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(err.into());
                }
            }
        };
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        let block = Block::new(data.into_boxed_slice(), cid);
        Ok(Some(block))
    }

    async fn put(&self, block: Block) -> Result<Cid, Error> {
        let path = block_path(self.path.clone(), &block.cid());
        let cids = self.cids.clone();
        let mut file = fs::File::create(path).await?;
        let data = block.data();
        file.write_all(&*data).await?;
        cids.lock().unwrap().insert(block.cid().to_owned());
        Ok(block.cid().to_owned())
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let path = block_path(self.path.clone(), cid);
        let cid = cid.to_owned();
        let cids = self.cids.clone();
        if cids.lock().unwrap().remove(&cid) {
            fs::remove_file(path).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "rocksdb")]
pub struct RocksDataStore {
    path: PathBuf,
    db: Arc<Mutex<Option<rocksdb::DB>>>,
}

#[cfg(feature = "rocksdb")]
trait ResolveColumnFamily {
    fn resolve<'a>(&self, db: &'a rocksdb::DB) -> &'a rocksdb::ColumnFamily;
}

#[cfg(feature = "rocksdb")]
impl ResolveColumnFamily for Column {
    fn resolve<'a>(&self, db: &'a rocksdb::DB) -> &'a rocksdb::ColumnFamily {
        let name = match *self {
            Column::Ipns => "ipns",
        };

        // not sure why this isn't always present?
        db.cf_handle(name).unwrap()
    }
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl DataStore for RocksDataStore {
    fn new(path: PathBuf) -> Self {
        RocksDataStore {
            path,
            db: Arc::new(Mutex::new(None)),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        let db = self.db.clone();
        let path = self.path.clone();
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let ipns_opts = rocksdb::Options::default();
        let ipns_cf = rocksdb::ColumnFamilyDescriptor::new("ipns", ipns_opts);
        let rdb = rocksdb::DB::open_cf_descriptors(&db_opts, &path, vec![ipns_cf])?;
        *db.lock().unwrap() = Some(rdb);
        Ok(())
    }

    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error> {
        let db = self.db.clone();
        let key = key.to_owned();
        let db = db.lock().unwrap();
        let db = db.as_ref().unwrap();
        let cf = col.resolve(db);
        let contains = db.get_cf(cf, &key)?.is_some();
        Ok(contains)
    }

    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let db = self.db.clone();
        let key = key.to_owned();
        let db = db.lock().unwrap();
        let db = db.as_ref().unwrap();
        let cf = col.resolve(db);
        let get = db.get_cf(cf, &key)?.map(|value| value.to_vec());
        Ok(get)
    }

    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let db = self.db.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        let db = db.lock().unwrap();
        let db = db.as_ref().unwrap();
        let cf = col.resolve(db);
        db.put_cf(cf, &key, &value)?;
        Ok(())
    }

    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error> {
        let db = self.db.clone();
        let key = key.to_owned();
        let db = db.lock().unwrap();
        let db = db.as_ref().unwrap();
        let cf = col.resolve(db);
        db.delete_cf(cf, &key)?;
        Ok(())
    }
}

fn block_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    let mut file = cid.to_string();
    file.push_str(".data");
    base.push(file);
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::async_test;
    use std::env::temp_dir;

    #[test]
    fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone().into());

        async_test(async move {
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

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(tmp.clone()).ok();

        let blockstore_path = tmp.clone();
        async_test(async move {
            let block = Block::from("1");

            let block_store = FsBlockStore::new(blockstore_path.clone().into());
            block_store.init().await.unwrap();
            block_store.open().await.unwrap();

            assert!(!block_store.contains(block.cid()).await.unwrap());
            block_store.put(block.clone()).await.unwrap();

            let block_store = FsBlockStore::new(blockstore_path.into());
            block_store.open().await.unwrap();
            assert!(block_store.contains(block.cid()).await.unwrap());
            assert_eq!(block_store.get(block.cid()).await.unwrap().unwrap(), block);
        });

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    #[cfg(feature = "rocksdb")]
    fn test_rocks_datastore() {
        let mut tmp = temp_dir();
        tmp.push("datastore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = RocksDataStore::new(tmp.clone().into());

        async_test(async move {
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

        std::fs::remove_dir_all(tmp).ok();
    }
}
