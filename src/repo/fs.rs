//! Persistent fs backed repo
use crate::block::{Cid, Block};
use crate::error::Error;
use crate::repo::{BlockStore, /*Column, DataStore*/};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use tokio_fs as fs;
use futures::stream::StreamExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;

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
            cids: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    async fn init(&self) -> Result<(), Error> {
        let path = self.path.clone();
        fs::create_dir_all(path).compat().await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        let path = self.path.clone();
        let cids = self.cids.clone();

        // not sure why this type needs to be specified
        let mut stream = fs::read_dir(path).compat().await?.compat();

        fn append_cid(cids: &Arc<Mutex<HashSet<Cid>>>, path: PathBuf) {
            if path.extension() != Some(OsStr::new("data")) {
                return;
            }
            let cid_str = path.file_stem().unwrap();
            let cid = Cid::from(cid_str.to_str().unwrap()).unwrap();
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
        let file = match fs::File::open(path).compat().await {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(err.into());
                }
            }
        };
        let (_, data) = tokio::io::read_to_end(file, Vec::new()).compat().await?;
        let block = Block::new(data, cid);
        Ok(Some(block))
    }

    async fn put(&self, block: Block) -> Result<Cid, Error> {
        let path = block_path(self.path.clone(), &block.cid());
        let cids = self.cids.clone();
        let file = fs::File::create(path).compat().await?;
        let data = block.data();
        tokio::io::write_all(file, &*data).compat().await?;
        cids.lock().unwrap().insert(block.cid().to_owned());
        Ok(block.cid().to_owned())
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let path = block_path(self.path.clone(), cid);
        let cid = cid.to_owned();
        let cids = self.cids.clone();
        if cids.lock().unwrap().remove(&cid) {
            fs::remove_file(path).compat().await?;
        }
        Ok(())
    }
}

/*
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
*/

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
    use futures::{FutureExt, TryFutureExt};

    #[test]
    fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone());

        tokio::runtime::current_thread::block_on_all(async move {
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
        }.unit_error().boxed().compat()).unwrap();

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(tmp.clone()).ok();

        let blockstore_path = tmp.clone();
        tokio::runtime::current_thread::block_on_all(async move {
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
        }.unit_error().boxed().compat()).unwrap();

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_rocks_datastore() {
        unimplemented!();
        /*
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
        */
    }
}
