//! Persistent fs backed repo
use crate::block::{Cid, Block};
use crate::error::Error;
use crate::repo::{BlockStore, Column, DataStore};
use futures::compat::*;
use futures::future::FutureObj;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::prelude::{Future as OldFuture, Stream as OldStream};
use tokio::fs;

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

    fn init(&self) -> FutureObj<'static, Result<(), Error>> {
        let path = self.path.clone();
        FutureObj::new(Box::new(async move {
            await!(fs::create_dir_all(path).compat())?;
            Ok(())
        }))
    }

    fn open(&self) -> FutureObj<'static, Result<(), Error>> {
        let path = self.path.clone();
        let cids = self.cids.clone();
        FutureObj::new(Box::new(async move {
            await!(fs::read_dir(path).flatten_stream().for_each(|dir| {
                let path = dir.path();
                if path.extension() == Some(OsStr::new("data")) {
                    let cid_str = path.file_stem().unwrap();
                    let cid = Cid::from(cid_str.to_str().unwrap()).unwrap();
                    cids.lock().unwrap().insert(cid);
                }
                Ok(())
            }).compat())?;
            Ok(())
        }))
    }

    fn contains(&self, cid: &Cid) -> FutureObj<'static, Result<bool, Error>> {
        let contains = self.cids.lock().unwrap().contains(cid);
        FutureObj::new(Box::new(async move {
            Ok(contains)
        })
    }

    fn get(&self, cid: &Cid) -> FutureObj<'static, Result<Option<Block>, Error>> {
        let path = block_path(self.path.clone(), cid);
        let cid = cid.to_owned();
        FutureObj::new(Box::new(async move {
            let file = match await!(fs::File::open(path).compat()) {
                Ok(file) => file,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        return Ok(None);
                    } else {
                        return Err(err.into());
                    }
                }
            };
            let (_, data) = await!(tokio::io::read_to_end(file, Vec::new()).compat())?;
            let block = Block::new(data, cid);
            Ok(Some(block))
        }))
    }

    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, Error>> {
        let path = block_path(self.path.clone(), &block.cid());
        let cids = self.cids.clone();
        FutureObj::new(Box::new(async move {
            let file = await!(fs::File::create(path).compat())?;
            let data = block.data();
            await!(tokio::io::write_all(file, &*data).compat())?;
            cids.lock().unwrap().insert(block.cid().to_owned());
            Ok(block.cid().to_owned())
        }))
    }

    fn remove(&self, cid: &Cid) -> FutureObj<'static, Result<(), Error>> {
        let path = block_path(self.path.clone(), cid);
        let cid = cid.to_owned();
        let cids = self.cids.clone();
        let contains = self.contains(&cid);
        FutureObj::new(Box::new(async move {
            if await!(contains)? {
                await!(fs::remove_file(path).compat())?;
                cids.lock().unwrap().remove(&cid);
            }
            Ok(())
        }))
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

    fn init(&self) -> FutureObj<'static, Result<(), Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
    }

    fn open(&self) -> FutureObj<'static, Result<(), Error>> {
        let db = self.db.clone();
        let path = self.path.clone();
        FutureObj::new(Box::new(async move {
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
        }))
    }

    fn contains(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<bool, Error>>
    {
        let cf = self.get_cf(col);
        let db = self.db.clone();
        let key = key.to_owned();
        FutureObj::new(Box::new(async move {
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            let contains = db.get_cf(cf, &key)?.is_some();
            Ok(contains)
        }))
    }

    fn get(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<Option<Vec<u8>>, Error>>
    {
        let cf = self.get_cf(col);
        let db = self.db.clone();
        let key = key.to_owned();
        FutureObj::new(Box::new(async move {
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            let get = db.get_cf(cf, &key)?.map(|value| value.to_vec());
            Ok(get)
        }))
    }

    fn put(&self, col: Column, key: &[u8], value: &[u8]) ->
        FutureObj<'static, Result<(), Error>>
    {
        let cf = self.get_cf(col);
        let db = self.db.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        FutureObj::new(Box::new(async move {
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            db.put_cf(cf, &key, &value)?;
            Ok(())
        }))
    }

    fn remove(&self, col: Column, key: &[u8]) ->
        FutureObj<'static, Result<(), Error>>
    {
        let cf = self.get_cf(col);
        let db = self.db.clone();
        let key = key.to_owned();
        FutureObj::new(Box::new(async move {
            let db = db.lock().unwrap();
            let db = db.as_ref().unwrap();
            db.delete_cf(cf, &key)?;
            Ok(())
        }))
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
            await!(block_store.init()).unwrap();
            await!(block_store.open()).unwrap();

            assert!(!await!(block_store.contains(block.cid())).unwrap());
            await!(block_store.put(block.clone())).unwrap();

            let block_store = FsBlockStore::new(blockstore_path);
            await!(block_store.open()).unwrap();
            assert!(await!(block_store.contains(block.cid())).unwrap());
            assert_eq!(await!(block_store.get(block.cid())).unwrap().unwrap(), block);
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

        std::fs::remove_dir_all(tmp).ok();
    }
}
