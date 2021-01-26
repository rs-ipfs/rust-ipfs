//! Storage implementation(s) backing the [`crate::Ipfs`].
use crate::error::Error;
use crate::p2p::KadResult;
use crate::path::IpfsPath;
use crate::subscription::{RequestKind, SubscriptionFuture, SubscriptionRegistry};
use crate::{Block, IpfsOptions};
use async_trait::async_trait;
use cid::{self, Cid};
use core::convert::TryFrom;
use core::fmt::Debug;
use futures::channel::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use futures::sink::SinkExt;
use libp2p::core::PeerId;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{error, fmt, io};

#[macro_use]
#[cfg(test)]
mod common_tests;

pub mod fs;
pub mod kv;
pub mod mem;

/// Consolidates `BlockStore` and `DataStore` into a representation of storage.
pub trait RepoTypes: Send + Sync + 'static {
    /// Describes a blockstore.
    type TBlockStore: BlockStore;
    /// Describes a datastore.
    type TDataStore: DataStore;
    type TLock: Lock;
}

/// Configuration for a repo.
#[derive(Clone, Debug)]
pub struct RepoOptions {
    path: PathBuf,
}

impl From<&IpfsOptions> for RepoOptions {
    fn from(options: &IpfsOptions) -> Self {
        RepoOptions {
            path: options.ipfs_path.clone(),
        }
    }
}

/// Convenience for creating a new `Repo` from the `RepoOptions`.
pub fn create_repo<TRepoTypes: RepoTypes>(
    options: RepoOptions,
) -> (Repo<TRepoTypes>, Receiver<RepoEvent>) {
    Repo::new(options)
}

/// A wrapper for `Cid` that has a `Multihash`-based equality check.
#[derive(Debug)]
pub struct RepoCid(Cid);

impl PartialEq for RepoCid {
    fn eq(&self, other: &Self) -> bool {
        self.0.hash() == other.0.hash()
    }
}
impl Eq for RepoCid {}

impl Hash for RepoCid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash().hash(state)
    }
}

/// Describes the outcome of `BlockStore::put_block`.
#[derive(Debug, PartialEq, Eq)]
pub enum BlockPut {
    /// A new block was written to the blockstore.
    NewBlock,
    /// The block already exists.
    Existed,
}

/// Describes the outcome of `BlockStore::remove`.
#[derive(Debug)]
pub enum BlockRm {
    /// A block was successfully removed from the blockstore.
    Removed(Cid),
    // TODO: DownloadCancelled(Cid, Duration),
}

// pub struct BlockNotFound(Cid);
/// Describes the error variants for `BlockStore::remove`.
#[derive(Debug)]
pub enum BlockRmError {
    // TODO: Pinned(Cid),
    /// The `Cid` doesn't correspond to a block in the blockstore.
    NotFound(Cid),
}

/// This API is being discussed and evolved, which will likely lead to breakage.
// FIXME: why is this unpin? doesn't probably need to be since all of the futures are Box::pin'd.
#[async_trait]
pub trait BlockStore: Debug + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    /// FIXME: redundant and never called during initialization, which is expected to happen during [`init`].
    async fn open(&self) -> Result<(), Error>;
    /// Returns whether a block is present in the blockstore.
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    /// Returns a block from the blockstore.
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    /// Inserts a block in the blockstore.
    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error>;
    /// Removes a block from the blockstore.
    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error>;
    /// Returns a list of the blocks (Cids), in the blockstore.
    async fn list(&self) -> Result<Vec<Cid>, Error>;
    /// Wipes the blockstore.
    async fn wipe(&self);
}

#[async_trait]
/// Generic layer of abstraction for a key-value data store.
pub trait DataStore: PinStore + Debug + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    /// Checks if a key is present in the datastore.
    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error>;
    /// Returns the value associated with a key from the datastore.
    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    /// Puts the value under the key in the datastore.
    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error>;
    /// Removes a key-value pair from the datastore.
    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error>;
    /// Wipes the datastore.
    async fn wipe(&self);
}

/// Errors variants describing the possible failures for `Lock::try_exclusive`.
#[derive(Debug)]
pub enum LockError {
    RepoInUse,
    LockFileOpenFailed(io::Error),
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            LockError::RepoInUse => "The repository is already being used by an IPFS instance.",
            LockError::LockFileOpenFailed(_) => "Failed to open repository lock file.",
        };

        write!(f, "{}", msg)
    }
}

impl From<io::Error> for LockError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            // `WouldBlock` is not used by `OpenOptions` (this could change), and can therefore be
            // matched on for the fs2 error in `FsLock::try_exclusive`.
            io::ErrorKind::WouldBlock => LockError::RepoInUse,
            _ => LockError::LockFileOpenFailed(error),
        }
    }
}

impl error::Error for LockError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        if let Self::LockFileOpenFailed(error) = self {
            Some(error)
        } else {
            None
        }
    }
}

/// A trait for describing repository locking.
///
/// This ensures no two IPFS nodes can be started with the same peer ID, as exclusive access to the
/// repository is guarenteed. This is most useful when using an fs backed repo.
pub trait Lock: Debug + Send + Sync {
    fn new(path: PathBuf) -> Self;
    fn try_exclusive(&mut self) -> Result<(), LockError>;
}

type References<'a> = futures::stream::BoxStream<'a, Result<Cid, crate::refs::IpldRefsError>>;

#[async_trait]
pub trait PinStore: Debug + Send + Sync + Unpin + 'static {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error>;

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error>;

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error>;

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error>;

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error>;

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>>;

    // here we should have resolved ids
    // go-ipfs: doesnt start fetching the paths
    // js-ipfs: starts fetching paths
    // FIXME: there should probably be an additional Result<$inner, Error> here; the per pin error
    // is serde OR cid::Error.
    /// Returns error if any of the ids isn't pinned in the required type, otherwise returns
    /// the pin details if all of the cids are pinned in one way or the another.
    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum Column {
    Ipns,
}

/// `PinMode` is the description of pin type for quering purposes.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PinMode {
    Indirect,
    Direct,
    Recursive,
}

/// Helper for around the quite confusing test required in [`PinStore::list`] and
/// [`PinStore::query`].
#[derive(Debug, Clone, Copy)]
enum PinModeRequirement {
    Only(PinMode),
    Any,
}

impl From<Option<PinMode>> for PinModeRequirement {
    fn from(filter: Option<PinMode>) -> Self {
        match filter {
            Some(one) => PinModeRequirement::Only(one),
            None => PinModeRequirement::Any,
        }
    }
}

impl PinModeRequirement {
    fn is_indirect_or_any(&self) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(PinMode::Indirect) | Any => true,
            Only(_) => false,
        }
    }

    fn matches<P: PartialEq<PinMode>>(&self, other: &P) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(one) if other == one => true,
            Only(_) => false,
            Any => true,
        }
    }

    fn required(&self) -> Option<PinMode> {
        use PinModeRequirement::*;
        match self {
            Only(one) => Some(*one),
            Any => None,
        }
    }
}

impl<B: Borrow<Cid>> PartialEq<PinMode> for PinKind<B> {
    fn eq(&self, other: &PinMode) -> bool {
        matches!((self, other),
            (PinKind::IndirectFrom(_), PinMode::Indirect)
            | (PinKind::Direct, PinMode::Direct)
            | (PinKind::Recursive(_), PinMode::Recursive)
            | (PinKind::RecursiveIntention, PinMode::Recursive))
    }
}

/// `PinKind` is more specific pin description for writing purposes. Implements
/// `PartialEq<&PinMode>`. Generic over `Borrow<Cid>` to allow storing both reference and owned
/// value of Cid.
#[derive(Debug, PartialEq, Eq)]
pub enum PinKind<C: Borrow<Cid>> {
    IndirectFrom(C),
    Direct,
    Recursive(u64),
    RecursiveIntention,
}

impl<C: Borrow<Cid>> PinKind<C> {
    fn as_ref(&self) -> PinKind<&'_ Cid> {
        use PinKind::*;
        match self {
            IndirectFrom(c) => PinKind::IndirectFrom(c.borrow()),
            Direct => PinKind::Direct,
            Recursive(count) => PinKind::Recursive(*count),
            RecursiveIntention => PinKind::RecursiveIntention,
        }
    }
}

/// Describes a repo.
///
/// Consolidates a blockstore, a datastore and a subscription registry.
#[derive(Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    block_store: TRepoTypes::TBlockStore,
    data_store: TRepoTypes::TDataStore,
    events: Sender<RepoEvent>,
    pub(crate) subscriptions: SubscriptionRegistry<Block, String>,
    lockfile: Arc<Mutex<TRepoTypes::TLock>>,
}

/// Events used to communicate to the swarm on repo changes.
#[derive(Debug)]
pub enum RepoEvent {
    /// Signals a desired block.
    WantBlock(Cid),
    /// Signals a desired block is no longer wanted.
    UnwantBlock(Cid),
    /// Signals the posession of a new block.
    NewBlock(
        Cid,
        oneshot::Sender<Result<SubscriptionFuture<KadResult, String>, anyhow::Error>>,
    ),
    /// Signals the removal of a block.
    RemovedBlock(Cid),
}

impl TryFrom<RequestKind> for RepoEvent {
    type Error = &'static str;

    fn try_from(req: RequestKind) -> Result<Self, Self::Error> {
        if let RequestKind::GetBlock(cid) = req {
            Ok(RepoEvent::UnwantBlock(cid))
        } else {
            Err("logic error: RepoEvent can only be created from a Request::GetBlock")
        }
    }
}

impl<TRepoTypes: RepoTypes> Repo<TRepoTypes> {
    pub fn new(options: RepoOptions) -> (Self, Receiver<RepoEvent>) {
        let mut blockstore_path = options.path.clone();
        let mut datastore_path = options.path.clone();
        let mut lockfile_path = options.path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        lockfile_path.push("repo_lock");

        let block_store = TRepoTypes::TBlockStore::new(blockstore_path);
        let data_store = TRepoTypes::TDataStore::new(datastore_path);
        let lockfile = TRepoTypes::TLock::new(lockfile_path);
        let (sender, receiver) = channel(1);

        (
            Repo {
                block_store,
                data_store,
                events: sender,
                subscriptions: Default::default(),
                lockfile: Arc::new(Mutex::new(lockfile)),
            },
            receiver,
        )
    }

    /// Shutdowns the repo, cancelling any pending subscriptions; Likely going away after some
    /// refactoring, see notes on [`crate::Ipfs::exit_daemon`].
    pub fn shutdown(&self) {
        self.subscriptions.shutdown();
    }

    pub async fn init(&self) -> Result<(), Error> {
        // Dropping the guard (even though not strictly necessary to compile) to avoid potential
        // deadlocks if `block_store` or `data_store` were to try to access `Repo.lockfile`.
        {
            let mut guard = self.lockfile.lock().unwrap();
            guard.try_exclusive()?;
        }

        let f1 = self.block_store.init();
        let f2 = self.data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    pub async fn open(&self) -> Result<(), Error> {
        let f1 = self.block_store.open();
        let f2 = self.data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    /// Puts a block into the block store.
    pub async fn put_block(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let cid = block.cid.clone();
        let (_cid, res) = self.block_store.put(block.clone()).await?;

        // FIXME: this doesn't cause actual DHT providing yet, only some
        // bitswap housekeeping; we might want to not ignore the channel
        // errors when we actually start providing on the DHT
        if let BlockPut::NewBlock = res {
            self.subscriptions
                .finish_subscription(cid.clone().into(), Ok(block));

            // sending only fails if no one is listening anymore
            // and that is okay with us.
            let (tx, rx) = oneshot::channel();

            self.events
                .clone()
                .send(RepoEvent::NewBlock(cid.clone(), tx))
                .await
                .ok();

            if let Ok(Ok(kad_subscription)) = rx.await {
                kad_subscription.await?;
            }
        }

        Ok((cid, res))
    }

    /// Retrives a block from the block store, or starts fetching it from the network and awaits
    /// until it has been fetched.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        // FIXME: here's a race: block_store might give Ok(None) and we get to create our
        // subscription after the put has completed. So maybe create the subscription first, then
        // cancel it?
        if let Some(block) = self.get_block_now(&cid).await? {
            Ok(block)
        } else {
            let subscription = self
                .subscriptions
                .create_subscription(cid.clone().into(), Some(self.events.clone()));
            // sending only fails if no one is listening anymore
            // and that is okay with us.
            self.events
                .clone()
                .send(RepoEvent::WantBlock(cid.clone()))
                .await
                .ok();
            Ok(subscription.await?)
        }
    }

    /// Retrieves a block from the block store if it's available locally.
    pub async fn get_block_now(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        self.block_store.get(&cid).await
    }

    /// Lists the blocks in the blockstore.
    pub async fn list_blocks(&self) -> Result<Vec<Cid>, Error> {
        self.block_store.list().await
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid) -> Result<Cid, Error> {
        if self.is_pinned(&cid).await? {
            return Err(anyhow::anyhow!("block to remove is pinned"));
        }

        // FIXME: Need to change location of pinning logic.
        // I like this pattern of the repo abstraction being some sort of
        // "clearing house" for the underlying result enums, but this
        // could potentially be pushed out out of here up to Ipfs, idk
        match self.block_store.remove(&cid).await? {
            Ok(success) => match success {
                BlockRm::Removed(_cid) => {
                    // sending only fails if the background task has exited
                    self.events
                        .clone()
                        .send(RepoEvent::RemovedBlock(cid.clone()))
                        .await
                        .ok();
                    Ok(cid.clone())
                }
            },
            Err(err) => match err {
                BlockRmError::NotFound(_cid) => Err(anyhow::anyhow!("block not found")),
            },
        }
    }

    /// Get an ipld path from the datastore.
    pub async fn get_ipns(&self, ipns: &PeerId) -> Result<Option<IpfsPath>, Error> {
        use std::str::FromStr;

        let data_store = &self.data_store;
        let key = ipns.to_owned();
        // FIXME: needless vec<u8> creation
        let bytes = data_store.get(Column::Ipns, &key.to_bytes()[..]).await?;
        match bytes {
            Some(ref bytes) => {
                let string = String::from_utf8_lossy(bytes);
                let path = IpfsPath::from_str(&string)?;
                Ok(Some(path))
            }
            None => Ok(None),
        }
    }

    /// Put an ipld path into the datastore.
    pub async fn put_ipns(&self, ipns: &PeerId, path: &IpfsPath) -> Result<(), Error> {
        let string = path.to_string();
        let value = string.as_bytes();
        // FIXME: needless vec<u8> creation
        self.data_store
            .put(Column::Ipns, &ipns.to_bytes()[..], value)
            .await
    }

    /// Remove an ipld path from the datastore.
    pub async fn remove_ipns(&self, ipns: &PeerId) -> Result<(), Error> {
        // FIXME: us needing to clone the peerid is wasteful to pass it as a reference only to be
        // cloned again
        self.data_store
            .remove(Column::Ipns, &ipns.to_bytes()[..])
            .await
    }

    /// Inserts a direct pin for a `Cid`.
    pub async fn insert_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.insert_direct_pin(cid).await
    }

    /// Inserts a recursive pin for a `Cid`.
    pub async fn insert_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        self.data_store.insert_recursive_pin(cid, refs).await
    }

    /// Removes a direct pin for a `Cid`.
    pub async fn remove_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.remove_direct_pin(cid).await
    }

    /// Removes a recursive pin for a `Cid`.
    pub async fn remove_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        // FIXME: not really sure why is there not an easier way to to transfer control
        self.data_store.remove_recursive_pin(cid, refs).await
    }

    /// Checks if a `Cid` is pinned.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        self.data_store.is_pinned(&cid).await
    }

    pub async fn list_pins(
        &self,
        mode: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        self.data_store.list(mode).await
    }

    pub async fn query_pins(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        self.data_store.query(cids, requirement).await
    }
}
