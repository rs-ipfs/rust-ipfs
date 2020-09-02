//! Persistent fs backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use async_trait::async_trait;
use bitswap::Block;
use cid::Cid;
use core::convert::TryFrom;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::{BlockRm, BlockRmError, RepoCid};

#[derive(Debug)]
pub struct FsBlockStore {
    path: PathBuf,
    cids: Mutex<HashSet<RepoCid>>,
    written_bytes: AtomicU64,
}

#[async_trait]
impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
            cids: Default::default(),
            written_bytes: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        let path = self.path.clone();
        fs::create_dir_all(path).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        let path = &self.path;
        let cids = &self.cids;

        let mut stream = fs::read_dir(path).await?;

        async fn append_cid(cids: &Mutex<HashSet<RepoCid>>, path: PathBuf) {
            if path.extension() != Some(OsStr::new("data")) {
                return;
            }
            let cid_str = path.file_stem().unwrap();
            let cid = Cid::try_from(cid_str.to_str().unwrap()).unwrap();
            cids.lock().await.insert(RepoCid(cid));
        }

        loop {
            let dir = stream.next().await;

            match dir {
                Some(Ok(dir)) => append_cid(&cids, dir.path()).await,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let contains = self.cids.lock().await.contains(&RepoCid(cid.to_owned()));
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

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let path = block_path(self.path.clone(), &block.cid());
        let cids = &self.cids;
        let data = block.data();

        let written = {
            let mut file = fs::File::create(path).await?;
            file.write_all(&*data).await?;
            file.flush().await?;
            data.len()
        };

        let retval = if cids.lock().await.insert(RepoCid(block.cid().to_owned())) {
            BlockPut::NewBlock
        } else {
            BlockPut::Existed
        };

        // FIXME: checking if the file existed already while creating complicates this function a
        // lot; might be better to just guard with mutex to enforce single task file access.. the
        // current implementation will write over the same file multiple times, each time believing
        // it was the first.

        self.written_bytes
            .fetch_add(written as u64, Ordering::SeqCst);

        Ok((block.cid, retval))
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        let path = block_path(self.path.clone(), cid);
        let cids = &self.cids;

        // We want to panic if there's a mutex unlock error
        // TODO: Check for pinned blocks here? Instead of repo?
        if cids.lock().await.remove(&RepoCid(cid.to_owned())) {
            fs::remove_file(path).await?;
            Ok(Ok(BlockRm::Removed(cid.clone())))
        } else {
            Ok(Err(BlockRmError::NotFound(cid.clone())))
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        // unwrapping as we want to panic on poisoned lock
        let guard = self.cids.lock().await;
        Ok(guard.iter().map(|cid| cid.0.clone()).collect())
    }

    async fn wipe(&self) {
        self.cids.lock().await.clear();
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
    use bitswap::Block;
    use cid::{Cid, Codec};
    use multihash::Sha2_256;
    use std::env::temp_dir;

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone());

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

        std::fs::remove_dir_all(tmp).ok();
    }

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(&tmp).ok();

        let data = b"1".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();
        block_store.open().await.unwrap();

        assert!(!block_store.contains(block.cid()).await.unwrap());
        block_store.put(block.clone()).await.unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.open().await.unwrap();
        assert!(block_store.contains(block.cid()).await.unwrap());
        assert_eq!(block_store.get(block.cid()).await.unwrap().unwrap(), block);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore_list() {
        let mut tmp = temp_dir();
        tmp.push("blockstore_list");
        std::fs::remove_dir_all(&tmp).ok();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();
        block_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec().into_boxed_slice();
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data_slice));
            let block = Block::new(data_slice, cid);
            block_store.put(block.clone()).await.unwrap();
        }

        let cids = block_store.list().await.unwrap();
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(block_store.contains(cid).await.unwrap());
        }
    }
}
