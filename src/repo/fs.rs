//! Persistent fs backed repo
use crate::error::Error;
use async_trait::async_trait;
use cid::Cid;
use core::convert::TryFrom;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

use super::{BlockRm, BlockRmError, Column, DataStore, RepoCid};

/// The PinStore implementation for FsDataStore
mod pinstore;

/// The FsBlockStore implementation
mod blocks;
pub use blocks::FsBlockStore;

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

fn block_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // this is ascii always, and wasteful until we can drop the cid for multihash ... which is
    // probably soon, we just need turn /refs/local to use /pin/list.
    let key = if cid.version() == cid::Version::V1 {
        cid.to_string()
    } else {
        Cid::new_v1(cid.codec(), cid.hash().to_owned()).to_string()
    };

    shard(&mut base, &key);

    base.set_extension("data");
    base
}

fn filestem_to_block_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
    file_stem.and_then(|stem| stem.to_str()).and_then(|s| {
        // this isn't an interchangeable way to store esp. cidv0
        let cid = Cid::try_from(s);

        // it's very unlikely that we'd hit a valid file with "data" extension
        // which we did write so I'd say wrapping the Cid parsing error as
        // std::io::Error is highly unnecessary. if someone wants to
        // *keep* ".data" ending files in the block store we shouldn't
        // die over it.
        //
        // if we could, we would do a log_once here, if we could easily
        // do such thing. like a inode based global probabilistic
        // hashset.
        //
        // FIXME: add test

        cid.ok()
    })
}

/// Same as `block_path` except it doesn't canonicalize the cid to later version. The produced
/// filename must be converted back to `Cid` using [`filestem_to_pin_cid`].
fn pin_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // it might be illegal to to render cidv0 as base32
    let key: String = multibase::Base::Base32Lower.encode(cid.to_bytes());
    shard(&mut base, &key);
    base
}

/// Decodes the file stem produced by [`pin_path`], ignoring errors.
fn filestem_to_pin_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
    file_stem.and_then(|stem| stem.to_str()).and_then(|s| {
        // this isn't an interchangeable way to store esp. cidv0
        let bytes = multibase::Base::Base32Lower.decode(s).ok()?;
        let cid = Cid::try_from(bytes);

        // See filestem_to_block_cid for discusison on why the error is ignored
        cid.ok()
    })
}

/// second-to-last/2 sharding, just by taking the two substring from an ASCII encoded key string to
/// be prepended as the directory or "shard".
///
/// This is done so that the directories don't get
/// gazillion files in them, which would slow them down. For example, git does this with hex or
/// base16 representation of sha1.
///
/// This function does not care how the key has been encoded, it is enough to have ASCII characters
/// where the shard is selected.
fn shard(path: &mut PathBuf, key: &str) {
    let start = key.len() - 3;
    let shard = &key[start..start + 2];
    assert_eq!(key[start + 2..].len(), 1);
    path.push(shard);
    path.push(key);
}

#[test]
fn shard_example() {
    let mut path = std::path::PathBuf::from("some/path/somewhere");
    let key = "ABCDEFG";

    shard(&mut path, key);

    let expected = std::path::Path::new("some/path/somewhere/EF/ABCDEFG");
    assert_eq!(path, expected);
}
