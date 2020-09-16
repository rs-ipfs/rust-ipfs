//! IPFS repo
use crate::error::Error;
use crate::p2p::KadResult;
use crate::path::IpfsPath;
use crate::subscription::{RequestKind, SubscriptionFuture, SubscriptionRegistry};
use crate::IpfsOptions;
use async_trait::async_trait;
use bitswap::Block;
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

#[macro_use]
#[cfg(test)]
mod common_tests;

pub mod fs;
pub mod mem;

pub trait RepoTypes: Send + Sync + 'static {
    type TBlockStore: BlockStore;
    type TDataStore: DataStore;
}

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

pub fn create_repo<TRepoTypes: RepoTypes>(
    options: RepoOptions,
) -> (Repo<TRepoTypes>, Receiver<RepoEvent>) {
    Repo::new(options)
}

/// A wrapper for `Cid` that has a `Multihash`-based equality check
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

/// Describes the outcome of `BlockStore::put_block`
#[derive(Debug, PartialEq, Eq)]
pub enum BlockPut {
    /// A new block was written
    NewBlock,
    /// The block existed already
    Existed,
}

#[derive(Debug)]
pub enum BlockRm {
    Removed(Cid),
    // TODO: DownloadCancelled(Cid, Duration),
}

// pub struct BlockNotFound(Cid);

#[derive(Debug)]
pub enum BlockRmError {
    // TODO: Pinned(Cid),
    NotFound(Cid),
}

/// This API is being discussed and evolved, which will likely lead to breakage.
// FIXME: why is this unpin? doesn't probably need to be since all of the futures are Box::pin'd.
#[async_trait]
pub trait BlockStore: Debug + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error>;
    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error>;
    async fn list(&self) -> Result<Vec<Cid>, Error>;
    async fn wipe(&self);
}

#[async_trait]
pub trait DataStore: PinStore + Debug + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error>;
    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error>;
    async fn wipe(&self);
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
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PinMode {
    Indirect,
    Direct,
    Recursive,
}

impl<B: Borrow<Cid>> PartialEq<PinMode> for PinKind<B> {
    fn eq(&self, other: &PinMode) -> bool {
        match (self, other) {
            (PinKind::IndirectFrom(_), PinMode::Indirect)
            | (PinKind::Direct, PinMode::Direct)
            | (PinKind::Recursive(_), PinMode::Recursive)
            | (PinKind::RecursiveIntention, PinMode::Recursive) => true,
            _ => false,
        }
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

#[derive(Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    block_store: TRepoTypes::TBlockStore,
    data_store: TRepoTypes::TDataStore,
    events: Sender<RepoEvent>,
    pub(crate) subscriptions: SubscriptionRegistry<Block, String>,
}

/// Events used to communicate to the swarm on repo changes.
#[derive(Debug)]
pub enum RepoEvent {
    WantBlock(Cid),
    UnwantBlock(Cid),
    NewBlock(
        Cid,
        oneshot::Sender<Result<SubscriptionFuture<KadResult, String>, anyhow::Error>>,
    ),
    UnprovideBlock(Cid),
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
        let mut datastore_path = options.path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        let block_store = TRepoTypes::TBlockStore::new(blockstore_path);
        let data_store = TRepoTypes::TDataStore::new(datastore_path);
        let (sender, receiver) = channel(1);
        (
            Repo {
                block_store,
                data_store,
                events: sender,
                subscriptions: Default::default(),
            },
            receiver,
        )
    }

    /// Shutdowns the repo, cancelling any pending subscriptions; Likely going away after some
    /// refactoring, see notes on [`Ipfs::exit_daemon`].
    pub fn shutdown(&self) {
        self.subscriptions.shutdown();
    }

    pub async fn init(&self) -> Result<(), Error> {
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

    /// Retrives a block from the block store if it's available locally.
    pub async fn get_block_now(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        self.block_store.get(&cid).await
    }

    pub async fn list_blocks(&self) -> Result<Vec<Cid>, Error> {
        self.block_store.list().await
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid) -> Result<Cid, Error> {
        if self.is_pinned(&cid).await? {
            return Err(anyhow::anyhow!("block to remove is pinned"));
        }

        // sending only fails if the background task has exited
        self.events
            .clone()
            .send(RepoEvent::UnprovideBlock(cid.clone()))
            .await
            .ok();

        // FIXME: Need to change location of pinning logic.
        // I like this pattern of the repo abstraction being some sort of
        // "clearing house" for the underlying result enums, but this
        // could potentially be pushed out out of here up to Ipfs, idk
        match self.block_store.remove(&cid).await? {
            Ok(success) => match success {
                BlockRm::Removed(_cid) => Ok(cid.clone()),
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
        let bytes = data_store.get(Column::Ipns, key.as_bytes()).await?;
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
        self.data_store
            .put(Column::Ipns, ipns.as_bytes(), value)
            .await
    }

    /// Remove an ipld path from the datastore.
    pub async fn remove_ipns(&self, ipns: &PeerId) -> Result<(), Error> {
        self.data_store.remove(Column::Ipns, ipns.as_bytes()).await
    }

    pub async fn insert_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.insert_direct_pin(cid).await
    }

    pub async fn insert_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        self.data_store.insert_recursive_pin(cid, refs).await
    }

    pub async fn remove_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.remove_direct_pin(cid).await
    }

    pub async fn remove_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        // FIXME: not really sure why is there not an easier way to to transfer control
        self.data_store.remove_recursive_pin(cid, refs).await
    }

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
