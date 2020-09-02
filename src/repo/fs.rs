//! Persistent fs backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use async_trait::async_trait;
use bitswap::Block;
use cid::Cid;
use core::convert::TryFrom;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::fs;
use tokio::sync::broadcast;

use super::{BlockRm, BlockRmError};

// mod flatfs;

#[derive(Debug)]
pub struct FsBlockStore {
    path: PathBuf,
    writes: Arc<Mutex<HashMap<Cid, broadcast::Sender<Result<(), ()>>>>>,
    written_bytes: AtomicU64,
}

#[async_trait]
impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
            //cids: Default::default(),
            writes: Arc::new(Mutex::new(HashMap::with_capacity(8))),
            written_bytes: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        fs::create_dir_all(self.path.clone()).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        // TODO: we probably want to cache the file
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let path = block_path(self.path.clone(), cid);
        let metadata = match fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        Ok(metadata.is_file())
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        use std::collections::hash_map::Entry;
        use std::io::Read;

        let receiver_or_cid = match self
            .writes
            .lock()
            .expect("cannot support poisoned")
            .entry(cid.to_owned())
        {
            Entry::Occupied(oe) => Ok(oe.get().subscribe()),
            Entry::Vacant(ve) => Err(ve.into_key()),
        };

        let cid = match receiver_or_cid {
            Ok(mut rx) => {
                match rx.recv().await {
                    Ok(Ok(())) => { /* good, something was just written */ }
                    Ok(Err(_)) => {
                        // write failed
                        return Ok(None);
                    }
                    Err(broadcast::RecvError::Closed) => { /* we missed it, could be either way */ }
                    Err(broadcast::RecvError::Lagged(_)) => unreachable!(
                        "sending at most one message to the channel with capacity of one"
                    ),
                }

                cid.to_owned()
            }
            Err(cid) => cid,
        };

        let path = block_path(self.path.clone(), &cid);

        // probably best to do everything in the blocking thread if we are to issue multiple
        // syscalls
        tokio::task::spawn_blocking(|| {
            let mut file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(e.into());
                }
            };

            let len = file.metadata()?.len();

            let mut data = Vec::with_capacity(len as usize);
            file.read_to_end(&mut data)?;
            let block = Block::new(data.into_boxed_slice(), cid);
            Ok(Some(block))
        })
        .await?
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;

        let target_path = block_path(self.path.clone(), &block.cid());
        let cid = block.cid;
        let data = block.data;

        // why synchronize here? because when we lose the race we cant know if there was someone
        // else interested in writing this block or not

        let (tx, mut rx, created) = {
            let mut g = self.writes.lock().expect("cant support poisoned");

            match g.entry(cid.to_owned()) {
                Entry::Occupied(oe) => {
                    // someone is already writing this, nice
                    (oe.get().clone(), oe.get().subscribe(), false)
                }
                Entry::Vacant(ve) => {
                    // we might be the first, or then the block exists already
                    let (tx, rx) = broadcast::channel(1);
                    ve.insert(tx.clone());
                    (tx, rx, true)
                }
            }
        };

        fs::create_dir_all(
            target_path
                .parent()
                .expect("parent always exists as it's the sharding directory"),
        )
        .await?;

        // pick the next writer by chance
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target_path)
        {
            Ok(target) => {
                // we get to write first!
                let je = tokio::task::spawn_blocking(move || {
                    let temp_path = target_path.with_extension("tmp");

                    match write_through_tempfile(target, &target_path, temp_path, &data) {
                        Ok(()) => Ok::<_, std::io::Error>(data.len()),
                        Err(e) => {
                            let _ = std::fs::remove_file(target_path);
                            Err(e)
                        }
                    }
                })
                .await;

                let written = match je {
                    Ok(Ok(written)) => written,
                    Ok(Err(e)) => {
                        // write failed but hopefully the target was removed
                        // no point in trying to remove it now
                        // ignore if no one is listening
                        let _ = tx.send(Err(()));
                        return Err(Error::new(e).context("write failed"));
                    }
                    Err(e) => {
                        // blocking task panicked or the runtime is going down, but we don't know
                        // if the thread has stopped or not (like not)
                        return Err(e.into());
                    }
                };

                let _ = tx
                    .send(Ok(()))
                    .expect("this cannot fail as we have at least one receiver on stack");

                drop(tx);
                drop(rx);

                if created {
                    let mut g = self.writes.lock().expect("cannot support poisoned");
                    // last one turns off the lights; the explicit drops for tx and rx are there
                    // only to make sure they are dropped before this point; removing the value
                    // will remove the last sender for any loser waiting on the AlreadyExists arm.
                    g.remove(&cid).expect("must exist; we created it");
                }

                self.written_bytes
                    .fetch_add(written as u64, Ordering::SeqCst);

                Ok((cid, BlockPut::NewBlock))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // At least the following cases:
                // - the block existed already
                // - the block is being written to and we should await for this to complete

                drop(tx);

                if created {
                    // need to remove this *before* we start awaiting since otherwise the sender
                    // would still be alive
                    let mut g = self.writes.lock().expect("cannot support poisoned");
                    // note comment on Ok(target) arm
                    g.remove(&cid).expect("must exist; we created it");
                }

                let message = match rx.recv().await {
                    Ok(message) => message,
                    Err(broadcast::RecvError::Closed) => {
                        // there were never any write intention by any party, and we may have just
                        // closed the last sender above
                        Ok(())
                    }
                    Err(broadcast::RecvError::Lagged(_)) => {
                        unreachable!("broadcast channel should only be messaged once here")
                    }
                };

                drop(rx);

                if message.is_err() {
                    // could loop, however if one write failed, the next should probably
                    Err(anyhow::anyhow!("other concurrent write failed"))
                } else {
                    Ok((cid.to_owned(), BlockPut::Existed))
                }
            }
            Err(e) => Err(Error::new(e).context("creating target file failed")),
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        let path = block_path(self.path.clone(), cid);

        match fs::remove_file(path).await {
            // FIXME: not sure if theres any point in taking cid ownership here?
            Ok(()) => Ok(Ok(BlockRm::Removed(cid.to_owned()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(Err(BlockRmError::NotFound(cid.to_owned())))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        use futures::stream::TryStreamExt;

        let stream = fs::read_dir(self.path.clone()).await?;

        // FIXME: written as a stream to make the Vec be BoxStream<'static, Cid>
        let vec = stream
            .and_then(|d| async move {
                // map over the shard directories
                Ok(if d.file_type().await?.is_dir() {
                    futures::future::Either::Left(fs::read_dir(d.path()).await?)
                } else {
                    futures::future::Either::Right(futures::stream::empty())
                })
            })
            // flatten each
            .try_flatten()
            // convert the paths ending in ".data" into cid
            .try_filter_map(|d| {
                let name = d.file_name();
                let path: &std::path::Path = name.as_ref();

                futures::future::ready(if path.extension() != Some("data".as_ref()) {
                    Ok(None)
                } else {
                    let wrong_way = path.file_stem().and_then(|stem| stem.to_str()).map(|s| {
                        Cid::try_from(s)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    });
                    match wrong_way {
                        Some(Ok(cid)) => Ok(Some(cid)),
                        Some(Err(e)) => Err(e),
                        None => Ok(None),
                    }
                })
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(vec)
    }

    async fn wipe(&self) {}
}

fn block_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // this is ascii always
    let mut file = cid.to_string();

    // second-to-last/2
    let start = file.len() - 3;

    let shard = &file[start..start + 2];
    assert_eq!(file[start + 2..].len(), 1);
    base.push(shard);

    // not sure why set extension instead
    file.push_str(".data");
    base.push(file);
    base
}

fn write_through_tempfile(
    target: std::fs::File,
    target_path: impl AsRef<std::path::Path>,
    temp_path: impl AsRef<std::path::Path>,
    data: &[u8],
) -> Result<(), std::io::Error> {
    use std::io::Write;

    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    temp.write_all(&*data)?;
    temp.flush()?;

    // safe default
    temp.sync_all()?;

    drop(temp);
    drop(target);

    std::fs::rename(temp_path, target_path)?;

    // FIXME: there should be a directory fsync here as well

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitswap::Block;
    use cid::{Cid, Codec};
    use hex_literal::hex;
    use multihash::Sha2_256;
    use std::env::temp_dir;
    use std::sync::Arc;

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

        let contains = store.contains(&cid).await.unwrap();
        assert_eq!(contains, false);
        let get = store.get(&cid).await.unwrap();
        assert_eq!(get, None);
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

    #[tokio::test(max_threads = 1)]
    async fn race_to_insert_new() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("race_to_insert_new");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());
        single.init().await.unwrap();

        let single = Arc::new(single);

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block {
            cid,
            data: data.into(),
        };

        let count = 10;

        let (writes, existing) = race_to_insert_scenario(count, block, &single).await;

        let single = Arc::try_unwrap(single).unwrap();
        assert_eq!(single.written_bytes.into_inner(), 15);

        assert_eq!(writes, 1);
        assert_eq!(existing, count - 1);
    }

    #[tokio::test(max_threads = 1)]
    async fn race_to_insert_with_existing() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("race_to_insert_existing");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());
        single.init().await.unwrap();

        let single = Arc::new(single);

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block {
            cid,
            data: data.into(),
        };

        single.put(block.clone()).await.unwrap();

        assert_eq!(single.written_bytes.load(Ordering::SeqCst), 15);

        let count = 10;

        let (writes, existing) = race_to_insert_scenario(count, block, &single).await;

        let single = Arc::try_unwrap(single).unwrap();
        assert_eq!(single.written_bytes.into_inner(), 15);

        assert_eq!(writes, 0);
        assert_eq!(existing, count);
    }

    async fn race_to_insert_scenario(
        count: usize,
        block: Block,
        blockstore: &Arc<FsBlockStore>,
    ) -> (usize, usize) {
        let barrier = Arc::new(tokio::sync::Barrier::new(count));

        let join_handles = (0..count)
            .map(|_| {
                tokio::spawn({
                    let bs = Arc::clone(&blockstore);
                    let barrier = Arc::clone(&barrier);
                    let block = block.clone();
                    async move {
                        barrier.wait().await;
                        bs.put(block).await
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut writes = 0usize;
        let mut existing = 0usize;

        for jh in join_handles {
            let res = jh.await;

            match res {
                Ok(Ok((_, BlockPut::NewBlock))) => writes += 1,
                Ok(Ok((_, BlockPut::Existed))) => existing += 1,
                Ok(Err(e)) => println!("joinhandle err: {}", e),
                _ => unreachable!("join error"),
            }
        }

        (writes, existing)
    }
}
