//! Persistent fs backed repo
use crate::block::{Cid, Block};
use crate::error::Error;
use crate::repo::{BlockStore, Column, DataStore};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::future::{ Future };
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::pin::Pin;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::prelude::*;

#[derive(Clone, Debug)]
pub struct FsBlockStore {
    path: PathBuf,
    cids: Arc<Mutex<HashSet<Cid>>>,
}

impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
            cids: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    fn init<'s>(&'s self) -> Pin<Box<(dyn Future<Output = Result<(), Error>> + Send + 's)>> {
        Box::pin(async {
            let path = self.path.clone();
            fs::create_dir_all(path).await
        })
    }

    fn open<'s>(&'s self) -> Pin<Box<(dyn Future<Output = Result<(), failure::Error>> + Send + 's)>> {
        Box::pin(async move {
            let path = self.path.clone();
            let cids = self.cids.clone();
            let mut entries = fs::read_dir(path).await;
            while let Some(res) = entries.unwrap().next().await {
                let dir = res?;
                let path = dir.path();
                if path.extension() == Some(OsStr::new("data")) {
                    let cid_str = path.file_stem().unwrap();
                    let cid = Cid::from(cid_str.to_str().unwrap()).unwrap();
                    cids.lock().unwrap().insert(cid);
                }
            }
            Ok(())
        })
    }

    fn contains(&self, cid: &Cid) -> Pin<Box<Result<bool, Error>>> {
        Box::pin({
            let contains = self.cids.lock().unwrap().contains(cid);
            Ok(contains)
        })
    }

    fn get<'s>(&'s self, cid: &'s Cid) -> Pin<Box<(dyn Future<Output = Result<Option<Block>, Error>> + Send + 's)>> {
        Box::pin(async move {
            let path = block_path(self.path.clone(), cid);
            let cid = cid.to_owned();
            let file = match fs::File::open(path).await {
                Ok(file) => file,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        return Ok(None);
                    } else {
                        return Err(err.into());
                    }
                }
            };
            let mut data: Vec<u8> = Vec::new();
            let _ = AsyncReadExt::read_to_end(&mut file, &mut data).await?;
            let block = Block::new(data, cid);
            Ok(Some(block))
        })
    }

    fn put<'s>(&'s self, block: Block) -> Pin<Box<(dyn Future<Output = Result<Cid, Error>> + Send + 's)>> {
        Box::pin(async move {
            let path = block_path(self.path.clone(), &block.cid());
            let cids = self.cids.clone();
            let file = fs::File::create(path).await?;
            let data = block.data();
            AsyncWriteExt::write_all(&mut file, &mut data).await?;
            cids.lock().unwrap().insert(block.cid().to_owned());
            Ok(block.cid().to_owned())
        })
    }

    fn remove<'s>(&'s self, cid: &'s Cid) -> Pin<Box<(dyn Future<Output = Result<(), Error>> + 's)>> {
        Box::pin(async move {
            let path = block_path(self.path.clone(), cid);
            let cid = cid.to_owned();
            let cids = self.cids.clone();
            let contains = self.contains(&cid);
            match contains.await.unwrap() {
                true => {
                    fs::remove_file(path).await?;
                    cids.lock().unwrap().remove(&cid);
                    Ok(())
                },
                false => { Ok(()) }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct RocksDataStore {
    path: PathBuf,
    db: Arc<Mutex<Option<rocksdb::DB>>>,
}

impl RocksDataStore {
    fn get_cf(&self, col: Column) -> rocksdb::ColumnFamily {
        let cf_name = match col {
            Column::Ipns => "ipns"
        };
        self.db.lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cf_handle(cf_name)
            // TODO safe to unwrap
            .unwrap()
    }
}

impl DataStore for RocksDataStore {
    fn new(path: PathBuf) -> Self {
        RocksDataStore {
            path,
            db: Arc::new(Mutex::new(None)),
        }
    }

    fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    fn open(&self) -> dyn Future<Output = Result<(), Error>> {
        let db = self.db.clone();
        let path = self.path.clone();
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let ipns_opts = rocksdb::Options::default();
        let ipns_cf = rocksdb::ColumnFamilyDescriptor::new("ipns", ipns_opts);
        let rdb = rocksdb::DB::open_cf_descriptors(
            &db_opts,
            &path,
            vec![ipns_cf],
        )?;
        *db.lock().unwrap() = Some(rdb);
        Ok(())
    }

    fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error>
    {
        let cf = self.get_cf(col);
        let db = self.db.clone();
        let key = key.to_owned();
        let db = db.lock().unwrap();
        let db = db.as_ref().unwrap();
        let contains = db.get_cf(cf, &key)?.is_some();
        Ok(contains)
    }

    fn get(&self, col: Column, key: &[u8]) -> Pin<Box<Result<Option<Vec<u8>>, Error>>>
    {
        Box::pin({
            let cf = self.get_cf(col);
            let db = self.db.clone();
            let key = key.to_owned();
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            let get = db.get_cf(cf, &key)?.map(|value| value.to_vec());
            Ok(get)
        })
    }

    fn put<'s>(&'s self, col: Column, key: &'s [u8], value: &'s [u8]) -> Pin<Box<(dyn Future<Output = Result<(), failure::Error>> + Send + 's)>>
    {
        Box::pin(async move {
            let cf = self.get_cf(col);
            let db = self.db.clone();
            let key = key.to_owned();
            let value = value.to_owned();
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            db.put_cf(cf, &key, &value)?;
            Ok(())
        })
    }

    fn remove<'s>(&'s self, col: Column, key: &'s [u8]) -> Pin<Box<(dyn Future<Output = Result<(), failure::Error>> + Send + 's)>>
    {
        Box::pin(async move {
            let cf = self.get_cf(col);
            let db = self.db.clone();
            let key = key.to_owned();
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            db.delete_cf(cf, &key)?;
            Ok(())
        })
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
    use std::env::temp_dir;

    #[test]
    fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone());

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

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(tmp.clone()).ok();

        let blockstore_path = tmp.clone();
        tokio::run_async(async move {
            let block = Block::from("1");

            let block_store = FsBlockStore::new(blockstore_path.clone());
            block_store.init().await.unwrap();
            block_store.open().await.unwrap();

            assert!(!block_store.contains(block.cid()).await.unwrap());
            block_store.put(block.clone()).await.unwrap();

            let block_store = FsBlockStore::new(blockstore_path);
            block_store.open().await.unwrap();
            assert!(block_store.contains(block.cid()).await.unwrap());
            assert_eq!(block_store.get(block.cid()).await.unwrap().unwrap(), block);
        });

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_rocks_datastore() {
        let mut tmp = temp_dir();
        tmp.push("datastore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = RocksDataStore::new(tmp.clone());

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

        std::fs::remove_dir_all(tmp).ok();
    }
}
