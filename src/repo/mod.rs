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
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

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

#[async_trait]
pub trait PinStore: Debug + Send + Sync + Unpin + 'static {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error>;

    async fn insert_pin(&self, target: &Cid, kind: PinKind<'_>) -> Result<(), Error>;
    async fn remove_pin(&self, target: &Cid, kind: PinKind<'_>) -> Result<(), Error>;

    // TODO: list?
}

#[derive(Clone, Copy, Debug)]
pub enum Column {
    Ipns,
    Pin,
}

pub enum PinKind<'a> {
    IndirectFrom(&'a Cid),
    Direct,
    Recursive(u64),
}

#[derive(Debug)]
pub struct Repo<TRepoTypes: RepoTypes> {
    block_store: TRepoTypes::TBlockStore,
    data_store: TRepoTypes::TDataStore,
    events: Sender<RepoEvent>,
    pub(crate) subscriptions: SubscriptionRegistry<Block, String>,
}

#[derive(Debug)]
pub enum RepoEvent {
    WantBlock(Cid),
    UnwantBlock(Cid),
    ProvideBlock(
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
        self.subscriptions
            .finish_subscription(cid.clone().into(), Ok(block));

        // FIXME: this doesn't cause actual DHT providing yet, only some
        // bitswap housekeeping; RepoEvent::ProvideBlock should probably
        // be renamed to ::NewBlock and we might want to not ignore the
        // channel errors when we actually start providing on the DHT
        if let BlockPut::NewBlock = res {
            // sending only fails if no one is listening anymore
            // and that is okay with us.
            let (tx, rx) = oneshot::channel();

            self.events
                .clone()
                .send(RepoEvent::ProvideBlock(cid.clone(), tx))
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
        if let Some(block) = self.block_store.get(&cid).await? {
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
        Ok(self.block_store.get(&cid).await?)
    }

    pub async fn list_blocks(&self) -> Result<Vec<Cid>, Error> {
        Ok(self.block_store.list().await?)
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

    pub async fn pin_block(&self, cid: &Cid) -> Result<(), Error> {
        self.insert_pin(cid, PinKind::Direct).await
    }

    pub async fn unpin_block(&self, cid: &Cid) -> Result<(), Error> {
        self.remove_pin(cid, PinKind::Direct).await
    }

    pub async fn insert_pin(&self, cid: &Cid, kind: PinKind<'_>) -> Result<(), Error> {
        self.data_store.insert_pin(cid, kind).await
    }

    pub async fn remove_pin(&self, cid: &Cid, kind: PinKind<'_>) -> Result<(), Error> {
        self.data_store.remove_pin(cid, kind).await
    }

    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        self.data_store.is_pinned(&cid).await
    }
}

#[derive(Serialize, Deserialize)]
pub struct PinDocument {
    version: u8,
    direct: bool,
    // how many descendants; something to check when walking
    recursive: Option<u64>,
    // no further metadata necessary; cids are pinned by full cid
    cid_version: u8,
    indirect_by: Vec<String>,
}

impl PinDocument {
    fn update(&mut self, add: bool, kind: PinKind<'_>) -> Result<bool, PinUpdateError> {
        match kind {
            PinKind::IndirectFrom(root) => {
                let root = if root.version() == cid::Version::V1 {
                    root.to_string()
                } else {
                    // this is one more allocation
                    Cid::new_v1(root.codec(), root.hash().to_owned()).to_string()
                };

                let modified = if self.indirect_by.is_empty() {
                    self.indirect_by.push(root);
                    true
                } else {
                    let mut set = self
                        .indirect_by
                        .drain(..)
                        .collect::<std::collections::BTreeSet<_>>();

                    let modified = if add {
                        set.insert(root)
                    } else {
                        set.remove(&root)
                    };

                    self.indirect_by.extend(set.into_iter());
                    modified
                };

                Ok(modified)
            }
            PinKind::Direct => {
                let modified = self.direct != add;
                self.direct = add;
                Ok(modified)
            }
            PinKind::Recursive(descendants) => {
                match self.recursive {
                    Some(0) => self.recursive = Some(descendants),
                    Some(other) if other != descendants => {
                        return Err(PinUpdateError::ChangingNumberOfDescendants(other))
                    }
                    Some(_) => return Ok(false),
                    None => self.recursive = Some(descendants),
                }
                Ok(true)
            }
        }
    }

    fn can_remove(&self) -> bool {
        !self.direct && self.recursive.is_none() && self.indirect_by.is_empty()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PinUpdateError {
    #[error("cannot change the non-zero number of descendants from {}", .0)]
    ChangingNumberOfDescendants(u64),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::env::temp_dir;

    #[derive(Clone)]
    pub struct Types;

    impl RepoTypes for Types {
        type TBlockStore = mem::MemBlockStore;
        type TDataStore = mem::MemDataStore;
    }

    pub fn create_mock_repo() -> (Repo<Types>, Receiver<RepoEvent>) {
        let mut tmp = temp_dir();
        tmp.push("rust-ipfs-repo");
        let options: RepoOptions = RepoOptions { path: tmp };
        Repo::new(options)
    }

    // Using boxed error here instead of anyhow to futureproof; we don't really care what goes
    // wrong here
    //
    // FIXME: how to duplicate this for each?
    async fn inited_repo() -> Result<
        Repo<Types>, /*, Receiver<RepoEvent>)*/
        Box<dyn std::error::Error + Send + Sync + 'static>,
    > {
        let (mock, rx) = create_mock_repo();
        drop(rx);
        mock.init().await?;

        let (empty_cid, empty_file_block) = ipfs_unixfs::file::adder::FileAdder::default()
            .finish()
            .next()
            .unwrap();

        assert_eq!(
            empty_cid.to_string(),
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH"
        );

        let empty_len = empty_file_block.len();

        trace!("putting in empty block");
        mock.put_block(Block {
            cid: empty_cid.clone(),
            data: empty_file_block.into(),
        })
        .await
        .unwrap();

        let mut tree_builder = ipfs_unixfs::dir::builder::BufferingTreeBuilder::default();

        let empty_paths = [
            "root/nested/deeper/an_empty_file",
            "root/nested/deeper/another_empty",
            // clone is just a copy of deeper but it's easier to build by copypasting this
            "root/clone_of_deeper/an_empty_file",
            "root/clone_of_deeper/another_empty",
        ];

        empty_paths
            .iter()
            .inspect(|p| println!("{:>50}: {}", p, empty_cid.clone()))
            .try_for_each(|p| tree_builder.put_link(p, empty_cid.clone(), empty_len as u64))
            .unwrap();

        for node in tree_builder.build() {
            let node = node.unwrap();
            println!("{:>50}: {}", node.path, node.cid);
            let block = Block {
                cid: node.cid,
                data: node.block,
            };

            mock.put_block(block).await.unwrap();
        }

        Ok(mock)
    }

    #[tokio::test(max_threads = 1)]
    async fn test_repo() {
        // FIXME: unsure what does this test
        let (repo, _) = create_mock_repo();
        repo.init().await.unwrap();
    }

    #[tokio::test(max_threads = 1)]
    async fn pin_direct_twice_is_good() {
        // let _ = tracing_subscriber::fmt::try_init();
        let repo = inited_repo().await.unwrap();

        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        assert_eq!(repo.is_pinned(&empty).await.unwrap(), false);

        repo.insert_pin(&empty, PinKind::Direct).await.unwrap();

        assert_eq!(repo.is_pinned(&empty).await.unwrap(), true);

        repo.insert_pin(&empty, PinKind::Direct).await.unwrap();

        assert_eq!(repo.is_pinned(&empty).await.unwrap(), true);
    }

    #[tokio::test(max_threads = 1)]
    async fn pin_recursive_pins_all_blocks() {
        let repo = inited_repo().await.unwrap();

        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        // assumed use:
        repo.insert_pin(&empty, PinKind::IndirectFrom(&root))
            .await
            .unwrap();

        // once or twice, doesn't matter
        repo.insert_pin(&empty, PinKind::IndirectFrom(&root))
            .await
            .unwrap();

        // the count can be either unique or include duplicates, I guess we just need to be
        // consistent
        repo.insert_pin(&root, PinKind::Recursive(1)).await.unwrap();

        assert!(repo.is_pinned(&root).await.unwrap());
        assert!(repo.is_pinned(&empty).await.unwrap());

        todo!("should look like indirect")
    }

    #[tokio::test(max_threads = 1)]
    async fn indirect_can_be_pinned_directly_but_remains_looking_indirect() {
        todo!("it will be both, but only show up as indirect")
    }

    #[tokio::test(max_threads = 1)]
    async fn direct_and_indirect_when_parent_unpinned() {
        todo!("it should become back directly pinned")
    }

    #[tokio::test(max_threads = 1)]
    async fn cannot_pin_recursively_pinned_directly() {
        // this is a bit of odd as other ops are additive
        todo!()
    }

    #[tokio::test(max_threads = 1)]
    async fn can_pin_direct_as_indirect() {
        todo!()
    }

    #[tokio::test(max_threads = 1)]
    async fn can_pin_direct_as_recursive() {
        // the other way around doesn't work
        todo!()
    }

    #[tokio::test(max_threads = 1)]
    async fn cannot_unpin_indirect() {
        todo!("error should read the pinned parent or first pinned parent")
    }

    #[tokio::test(max_threads = 1)]
    async fn cannot_unpin_not_pinned() {
        todo!("should this still fetch the block at upper level?")
    }
}
