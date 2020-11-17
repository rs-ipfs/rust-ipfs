//! Persistent fs backed repo.
//!
//! Consists of [`FsDataStore`] and [`FsBlockStore`].

use crate::error::Error;
use async_trait::async_trait;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::sync::Semaphore;

use super::{BlockRm, BlockRmError, Column, DataStore, Lock, LockError, RepoCid};

/// The PinStore implementation for FsDataStore
mod pinstore;

/// The FsBlockStore implementation
mod blocks;
pub use blocks::FsBlockStore;

/// Path mangling done for pins and blocks
mod paths;
use paths::{block_path, filestem_to_block_cid, filestem_to_pin_cid, pin_path};

/// FsDataStore which uses the filesystem as a lockable key-value store. Maintains a similar to
/// [`FsBlockStore`] sharded two level storage. Direct have empty files, recursive pins record all of
/// their indirect descendants. Pin files are separated by their file extensions.
///
/// When modifying, single lock is used.
///
/// For the [`crate::repo::PinStore`] implementation see `fs/pinstore.rs`.
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

#[derive(Debug)]
pub struct FsLock {
    file: Option<File>,
    path: PathBuf,
    state: State,
}

#[derive(Debug)]
enum State {
    Unlocked,
    Exclusive,
}

impl Lock for FsLock {
    fn new(path: PathBuf) -> Self {
        Self {
            file: None,
            path,
            state: State::Unlocked,
        }
    }

    fn try_exclusive(&mut self) -> Result<(), LockError> {
        use fs2::FileExt;
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)?;

        file.try_lock_exclusive()?;

        self.state = State::Exclusive;
        self.file = Some(file);

        Ok(())
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(common_tests, crate::repo::fs::FsDataStore::new);

#[cfg(test)]
mod tests {
    use super::{FsLock, Lock};

    #[test]
    fn creates_an_exclusive_repo_lock() {
        let temp_dir = std::env::temp_dir();
        let lockfile_path = temp_dir.join("repo_lock");

        let mut lock = FsLock::new(lockfile_path.clone());
        let result = lock.try_exclusive();
        assert_eq!(result.is_ok(), true);

        let mut failing_lock = FsLock::new(lockfile_path.clone());
        let result = failing_lock.try_exclusive();
        assert_eq!(result.is_err(), true);

        // Clean-up.
        std::fs::remove_file(lockfile_path).unwrap();
    }
}
