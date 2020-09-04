//! Persistent fs backed repo
use crate::error::Error;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::sync::Semaphore;

use super::{BlockRm, BlockRmError, Column, DataStore, RepoCid};

/// The PinStore implementation for FsDataStore
mod pinstore;

/// The FsBlockStore implementation
mod blocks;
pub use blocks::FsBlockStore;

/// Path mangling done for pins and blocks
mod paths;
use paths::{block_path, filestem_to_block_cid, filestem_to_pin_cid, pin_path};

/// FsDataStore which uses the filesystem as a lockable key-value store. Maintains a similar to
/// blockstore sharded two level storage. Direct have empty files, recursive pins record all of
/// their indirect descendants. Pin files are separated by their file extensions.
///
/// When modifying, single write lock is used.
///
/// For the PinStore implementation, please see `fs/pinstore.rs`.
#[derive(Debug)]
pub struct FsDataStore {
    /// The base directory under which we have a sharded directory structure, and the individual
    /// blocks are stored under the shard. See unixfs/examples/cat.rs for read example.
    path: PathBuf,

    /// Start with simple, conservative solution, allows concurrent queries but single writer.
    /// It is assumed the reads do not require permit as non-empty writes are done through
    /// tempfiles and the consistency regarding reads is not a concern right now. For garbage
    /// collection implementation, it might be needed to hold this permit for the duration of
    /// garbage collection, or something similar.
    lock: Arc<Semaphore>,

    /// Not really needed
    written_bytes: AtomicU64,
}

/// The column operations are all unimplemented pending at least downscoping of the
/// DataStore trait itself.
#[async_trait]
impl DataStore for FsDataStore {
    fn new(mut root: PathBuf) -> Self {
        root.push("pins");
        FsDataStore {
            path: root,
            lock: Arc::new(Semaphore::new(1)),
            written_bytes: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        tokio::fs::create_dir_all(&self.path).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, _col: Column, _key: &[u8]) -> Result<bool, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn get(&self, _col: Column, _key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn put(&self, _col: Column, _key: &[u8], _value: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn remove(&self, _col: Column, _key: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn wipe(&self) {
        todo!()
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(common_tests, crate::repo::fs::FsDataStore::new);
