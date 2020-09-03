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
use std::borrow::Borrow;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Recursive {
    /// Persistent record of **completed** recursive pinning. All references now have indirect pins
    /// recorded.
    Count(u64),
    /// Persistent record of intent to add recursive pins to all indirect blocks or even not to
    /// keep the go-ipfs way which might not be a bad idea after all. Adding all the indirect pins
    /// on disk will cause massive write amplification in the end, but lets keep that way until we
    /// get everything working at least.
    Intent,
    /// Not pinned recursively.
    Not,
}

impl Recursive {
    fn is_set(&self) -> bool {
        match self {
            Recursive::Count(_) | Recursive::Intent => true,
            Recursive::Not => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PinDocument {
    version: u8,
    direct: bool,
    // how many descendants; something to check when walking
    recursive: Recursive,
    // no further metadata necessary; cids are pinned by full cid
    cid_version: u8,
    // using the cidv1 versions of all cids here, not sure if that makes sense or is important
    indirect_by: Vec<String>,
    // supporting all kinds of directions here; this is a list of the Cids that are indirectly
    // pinned because of this recursive pinning.
    indirect_to: Vec<String>,
}

impl PinDocument {
    fn update(&mut self, add: bool, kind: &PinKind<&'_ Cid>) -> Result<bool, PinUpdateError> {
        // these update rules are a bit complex and there are cases we don't need to handle.
        // Updating on upon `PinKind` forces the caller to inspect what the current state is for
        // example to handle the case of failing "unpin currently recursively pinned as direct".
        // the ruleset seems quite strange to be honest.
        match kind {
            PinKind::IndirectFrom(root) => {
                let root = if root.version() == cid::Version::V1 {
                    root.to_string()
                } else {
                    // this is one more allocation
                    Cid::new_v1(root.codec(), (*root).hash().to_owned()).to_string()
                };

                let modified = if self.indirect_by.is_empty() {
                    if add {
                        self.indirect_by.push(root);
                        true
                    } else {
                        false
                    }
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
                if self.recursive.is_set() && !self.direct && add {
                    // go-ipfs: cannot make recursive pin also direct
                    // not really sure why does this rule exist; the other way around is allowed
                    return Err(PinUpdateError::AlreadyPinnedRecursive);
                }

                if !self.direct && !add {
                    panic!("this situation must be handled by the caller by checking that recursive pin is about to be removed as direct");
                }

                let modified = self.direct != add;
                self.direct = add;
                Ok(modified)
            }
            PinKind::RecursiveIntention => {
                let modified = if add {
                    match self.recursive {
                        Recursive::Count(_) => return Err(PinUpdateError::AlreadyPinnedRecursive),
                        // can overwrite Intent with another Intent, as Ipfs::insert_pin is now moving to fix
                        // the Intent into the "final form" of Recursive::Count.
                        Recursive::Intent => false,
                        Recursive::Not => {
                            self.recursive = Recursive::Intent;
                            self.direct = false;
                            true
                        }
                    }
                } else {
                    match self.recursive {
                        Recursive::Count(_) | Recursive::Intent => {
                            self.recursive = Recursive::Not;
                            true
                        }
                        Recursive::Not => false,
                    }
                };

                Ok(modified)
            }
            PinKind::Recursive(descendants) => {
                let descendants = *descendants;
                let modified = if add {
                    match self.recursive {
                        Recursive::Count(other) if other != descendants => {
                            return Err(PinUpdateError::UnexpectedNumberOfDescendants(
                                other,
                                descendants,
                            ))
                        }
                        Recursive::Count(_) => false,
                        Recursive::Intent | Recursive::Not => {
                            self.recursive = Recursive::Count(descendants);
                            // the previously direct has now been upgraded to recursive, it can
                            // still be indirect though
                            self.direct = false;
                            true
                        }
                    }
                } else {
                    match self.recursive {
                        Recursive::Count(other) if other != descendants => {
                            return Err(PinUpdateError::UnexpectedNumberOfDescendants(
                                other,
                                descendants,
                            ))
                        }
                        Recursive::Count(_) | Recursive::Intent => {
                            self.recursive = Recursive::Not;
                            true
                        }
                        Recursive::Not => return Err(PinUpdateError::NotPinnedRecursive),
                    }
                    // FIXME: removing ... not sure if this is an issue; was thinking that maybe
                    // the update might need to be split to allow different api for removal than
                    // addition.
                };
                Ok(modified)
            }
        }
    }

    fn can_remove(&self) -> bool {
        !self.direct && !self.recursive.is_set() && self.indirect_by.is_empty()
    }

    fn mode(&self) -> Option<PinMode> {
        if self.recursive.is_set() {
            Some(PinMode::Recursive)
        } else if !self.indirect_by.is_empty() {
            Some(PinMode::Indirect)
        } else if self.direct {
            Some(PinMode::Direct)
        } else {
            None
        }
    }

    fn pick_kind(&self) -> Option<Result<PinKind<Cid>, cid::Error>> {
        self.mode().map(|p| {
            Ok(match p {
                PinMode::Recursive => match self.recursive {
                    Recursive::Intent => PinKind::RecursiveIntention,
                    Recursive::Count(total) => PinKind::Recursive(total),
                    _ => unreachable!("mode shuold not have returned PinKind::Recursive"),
                },
                PinMode::Indirect => {
                    // go-ipfs does seem to be doing a fifo looking, perhaps this is a list there, or
                    // the indirect pins aren't being written down anywhere and they just refs from
                    // recursive roots.
                    let cid = Cid::try_from(self.indirect_by[0].as_str())?;
                    PinKind::IndirectFrom(cid)
                }
                PinMode::Direct => PinKind::Direct,
            })
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PinUpdateError {
    #[error("unexpected number of descendants ({}), found {}", .1, .0)]
    UnexpectedNumberOfDescendants(u64, u64),
    #[error("not pinned recursively")]
    NotPinnedRecursive,
    /// Not allowed: Adding direct pin while pinned recursive
    #[error("already pinned recursively")]
    AlreadyPinnedRecursive,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::stream::{StreamExt, TryStreamExt};
    use std::collections::HashMap;
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
            //.inspect(|p| println!("{:>50}: {}", p, empty_cid.clone()))
            .try_for_each(|p| tree_builder.put_link(p, empty_cid.clone(), empty_len as u64))
            .unwrap();

        for node in tree_builder.build() {
            let node = node.unwrap();
            //println!("{:>50}: {}", node.path, node.cid);
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

        repo.insert_direct_pin(&empty).await.unwrap();

        assert_eq!(repo.is_pinned(&empty).await.unwrap(), true);

        repo.insert_direct_pin(&empty).await.unwrap();

        assert_eq!(repo.is_pinned(&empty).await.unwrap(), true);
    }

    #[tokio::test(max_threads = 1)]
    async fn pin_recursive_pins_all_blocks() {
        let repo = inited_repo().await.unwrap();

        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        // assumed use:
        repo.insert_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        assert!(repo.is_pinned(&root).await.unwrap());
        assert!(repo.is_pinned(&empty).await.unwrap());

        let mut both = repo
            .list_pins(None)
            .await
            .try_collect::<HashMap<Cid, PinMode>>()
            .await
            .unwrap();

        assert_eq!(both.remove(&root), Some(PinMode::Recursive));
        assert_eq!(both.remove(&empty), Some(PinMode::Indirect));

        assert!(both.is_empty(), "{:?}", both);
    }

    #[tokio::test(max_threads = 1)]
    async fn indirect_can_be_pinned_directly_but_remains_looking_indirect() {
        let repo = inited_repo().await.unwrap();

        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        repo.insert_direct_pin(&empty).await.unwrap();

        repo.insert_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        let mut both = repo
            .list_pins(None)
            .await
            .try_collect::<HashMap<Cid, PinMode>>()
            .await
            .unwrap();

        assert_eq!(both.remove(&root), Some(PinMode::Recursive));
        assert_eq!(both.remove(&empty), Some(PinMode::Indirect));

        assert!(both.is_empty(), "{:?}", both);
    }

    #[tokio::test(max_threads = 1)]
    async fn direct_and_indirect_when_parent_unpinned() {
        let repo = inited_repo().await.unwrap();

        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        repo.insert_direct_pin(&empty).await.unwrap();

        assert_eq!(
            repo.query_pins(vec![empty.clone()], None)
                .await
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![(empty.clone(), PinKind::Direct)],
        );

        // first refs

        repo.insert_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        // second refs

        repo.remove_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        let mut one = repo
            .list_pins(None)
            .await
            .try_collect::<HashMap<Cid, PinMode>>()
            .await
            .unwrap();

        assert_eq!(one.remove(&empty), Some(PinMode::Direct));

        assert!(one.is_empty(), "{:?}", one);
    }

    #[tokio::test(max_threads = 1)]
    async fn cannot_pin_recursively_pinned_directly() {
        // this is a bit of odd as other ops are additive
        let repo = inited_repo().await.unwrap();

        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        repo.insert_recursive_pin(&empty, futures::stream::iter(vec![]).boxed())
            .await
            .unwrap();

        let e = repo.insert_direct_pin(&empty).await.unwrap_err();

        // go-ipfs puts the cid in front here, not sure if we want to at this level? though in
        // go-ipfs it's different than path resolving
        assert_eq!(e.to_string(), "already pinned recursively");
    }

    #[test]
    fn pindocument_on_direct_pin() {
        let mut doc = PinDocument {
            version: 0,
            direct: false,
            recursive: Recursive::Not,
            cid_version: 0,
            indirect_by: Vec::new(),
            indirect_to: Vec::new(),
        };

        assert!(doc.update(true, &PinKind::Direct).unwrap());

        assert_eq!(doc.mode(), Some(PinMode::Direct));
        assert_eq!(doc.pick_kind().unwrap().unwrap(), PinKind::Direct);
    }

    #[tokio::test(max_threads = 1)]
    async fn can_pin_direct_as_recursive() {
        // the other way around doesn't work
        let repo = inited_repo().await.unwrap();
        //
        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        repo.insert_direct_pin(&root).await.unwrap();

        let pins = repo
            .list_pins(None)
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(pins, vec![(root.clone(), PinMode::Direct)]);

        // first refs

        repo.insert_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        let mut both = repo
            .list_pins(None)
            .await
            .try_collect::<HashMap<Cid, PinMode>>()
            .await
            .unwrap();

        assert_eq!(both.remove(&root), Some(PinMode::Recursive));
        assert_eq!(both.remove(&empty), Some(PinMode::Indirect));

        assert!(both.is_empty(), "{:?}", both);
    }

    #[tokio::test(max_threads = 1)]
    #[should_panic(expected = "situation must be handled by the caller")]
    async fn cannot_unpin_indirect() {
        let repo = inited_repo().await.unwrap();
        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        // first refs

        repo.insert_recursive_pin(
            &root,
            futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
        )
        .await
        .unwrap();

        // should panic because the caller must not attempt this because:

        let (_, kind) = repo
            .query_pins(vec![empty.clone()], None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        // the cids are stored as v1 ... not sure if that makes any sense TBH
        // feels like Cid should be equal regardless of version.
        let root_v1 = Cid::new_v1(root.codec(), root.hash().to_owned());
        assert_eq!(kind, PinKind::IndirectFrom(root_v1));

        // this makes the "remove direct" invalid, as the direct pin must not be removed while
        // recursively pinned

        let _ = repo.remove_direct_pin(&empty).await;
        unreachable!("should have panicked");
    }

    #[tokio::test(max_threads = 1)]
    async fn cannot_unpin_not_pinned() {
        let repo = inited_repo().await.unwrap();
        // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        // the only pin we can try removing without first querying is direct, as shown in
        // `cannot_unpin_indirect`.

        let e = repo.remove_direct_pin(&empty).await.unwrap_err();

        // FIXME: go-ipfs errors on the actual path
        assert_eq!(e.to_string(), "not pinned");
    }
}
