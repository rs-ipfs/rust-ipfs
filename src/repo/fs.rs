//! Persistent fs backed repo
use crate::error::Error;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

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
    /// FIXME: this needs to become semaphore for owned permits; do "dirty reads" as there should
    /// not be any chance of seeing partial writes, that is, under modern linux filesystems.
    lock: tokio::sync::RwLock<()>,

    /// Not really needed
    written_bytes: AtomicU64,
}

// FIXME: need to do
//  - bring back the canonicalized version, or just force base32? should work with that as well
//  - block rm test failure
//  - refs local test failure

#[async_trait]
impl DataStore for FsDataStore {
    fn new(mut root: PathBuf) -> Self {
        root.push("pins");
        FsDataStore {
            path: root,
            lock: Default::default(),
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
