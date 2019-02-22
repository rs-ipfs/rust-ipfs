//! Persistent fs backed repo
use crate::block::{Cid, Block};
use crate::repo::{BlockStore, DataStore};
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

    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(fs::create_dir_all(self.path.clone()).compat()))
    }

    fn open(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        let path = self.path.clone();
        let cids = self.cids.clone();
        FutureObj::new(Box::new(async move {
            await!(fs::read_dir(path).flatten_stream().for_each(|dir| {
                let path = dir.path();
                if path.extension() == Some(OsStr::new("data")) {
                    let cid_str = path.file_stem().unwrap();
                    let cid = Arc::new(cid::Cid::from(cid_str.to_str().unwrap()).unwrap());
                    cids.lock().unwrap().insert(cid);
                }
                Ok(())
            }).compat())?;
            Ok(())
        }))
    }

    fn contains(&self, cid: Cid) -> FutureObj<'static, Result<bool, std::io::Error>> {
        let contains = self.cids.lock().unwrap().contains(&cid);
        FutureObj::new(Box::new(async move {
            Ok(contains)
        })
    }

    fn get(&self, cid: Cid) -> FutureObj<'static, Result<Option<Block>, std::io::Error>> {
        let path = block_path(self.path.clone(), &cid);
        FutureObj::new(Box::new(async move {
            let file = match await!(fs::File::open(path).compat()) {
                Ok(file) => file,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        return Ok(None);
                    } else {
                        return Err(err);
                    }
                }
            };
            let (_, data) = await!(tokio::io::read_to_end(file, Vec::new()).compat())?;
            let block = Block::new(data, (&*cid).to_owned());
            Ok(Some(block))
        }))
    }

    fn put(&self, block: Block) -> FutureObj<'static, Result<Cid, std::io::Error>> {
        let path = block_path(self.path.clone(), &block.cid());
        let cids = self.cids.clone();
        FutureObj::new(Box::new(async move {
            let file = await!(fs::File::create(path).compat())?;
            let data = block.data();
            await!(tokio::io::write_all(file, &*data).compat())?;
            cids.lock().unwrap().insert(block.cid());
            Ok(block.cid())
        }))
    }

    fn remove(&self, cid: Cid) -> FutureObj<'static, Result<(), std::io::Error>> {
        let path = block_path(self.path.clone(), &cid);
        let cids = self.cids.clone();
        FutureObj::new(Box::new(async move {
            await!(fs::remove_file(path).compat())?;
            cids.lock().unwrap().remove(&cid);
            Ok(())
        }))
    }
}

#[derive(Clone, Debug)]
pub struct RocksDataStore {
    path: PathBuf,
}

impl DataStore for RocksDataStore {
    fn new(path: PathBuf) -> Self {
        RocksDataStore {
            path
        }
    }

    fn init(&self) -> FutureObj<'static, Result<(), std::io::Error>> {
        FutureObj::new(Box::new(futures::future::ok(())))
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
    use futures::prelude::*;
    use std::env::temp_dir;

    #[test]
    fn test_fs_blockstore() {
        let block = Block::from("1");
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();

        let blockstore_path = tmp.clone();
        tokio::run(FutureObj::new(Box::new(async move {
            let block_store = FsBlockStore::new(blockstore_path);
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
            Ok(())
        })).compat());

        std::fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn test_fs_blockstore_open() {
        let block = Block::from("1");
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(tmp.clone()).ok();

        let blockstore_path = tmp.clone();
        tokio::run(FutureObj::new(Box::new(async move {
            let block_store = FsBlockStore::new(blockstore_path.clone());
            await!(block_store.init()).unwrap();
            await!(block_store.open()).unwrap();

            assert!(!await!(block_store.contains(block.cid())).unwrap());
            await!(block_store.put(block.clone())).unwrap();

            let block_store = FsBlockStore::new(blockstore_path);
            await!(block_store.open()).unwrap();
            assert!(await!(block_store.contains(block.cid())).unwrap());
            assert_eq!(await!(block_store.get(block.cid())).unwrap().unwrap(), block);
            Ok(())
        })).compat());

        std::fs::remove_dir_all(tmp).ok();
    }
}
