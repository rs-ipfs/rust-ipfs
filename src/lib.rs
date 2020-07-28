//! IPFS node implementation
//#![deny(missing_docs)]
// Recursion limit is required by the use of async_stream
#![recursion_limit = "512"]
#![cfg_attr(feature = "nightly", feature(external_doc))]
#![cfg_attr(feature = "nightly", doc(include = "../README.md"))]

#[macro_use]
extern crate log;

use anyhow::{anyhow, format_err};
use async_std::{path::PathBuf, task};
pub use bitswap::{BitswapEvent, Block, Stats};
pub use cid::Cid;
use cid::Codec;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use futures::sink::SinkExt;
use futures::stream::{Fuse, Stream};
pub use libipld::ipld::Ipld;
pub use libp2p::core::{connection::ListenerId, ConnectedPoint, Multiaddr, PeerId, PublicKey};
pub use libp2p::identity::Keypair;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use std::task::{Context, Poll};

mod config;
mod dag;
pub mod error;
pub mod ipns;
pub mod p2p;
pub mod path;
pub mod repo;
mod subscription;
pub mod unixfs;

use self::dag::IpldDag;
pub use self::error::Error;
use self::ipns::Ipns;
pub use self::p2p::pubsub::{PubsubMessage, SubscriptionStream};
pub use self::p2p::Connection;
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::path::IpfsPath;
pub use self::repo::RepoTypes;
use self::repo::{create_repo, Repo, RepoOptions};
use self::subscription::{RequestKind, SubscriptionFuture, SubscriptionId};
use self::unixfs::File;

/// All types can be changed at compile time by implementing
/// `IpfsTypes`.
pub trait IpfsTypes: SwarmTypes + RepoTypes {}
impl<T: RepoTypes> SwarmTypes for T {}
impl<T: SwarmTypes + RepoTypes> IpfsTypes for T {}

/// Default IPFS types.
#[derive(Debug, Clone)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    #[cfg(feature = "rocksdb")]
    type TDataStore = repo::fs::RocksDataStore;
    #[cfg(not(feature = "rocksdb"))]
    type TDataStore = repo::mem::MemDataStore;
}

/// Testing IPFS types
#[derive(Debug, Clone)]
pub struct TestTypes;
impl RepoTypes for TestTypes {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
}

/// Ipfs options
#[derive(Clone)]
pub struct IpfsOptions<Types: IpfsTypes> {
    _marker: PhantomData<Types>,
    /// The path of the ipfs repo.
    pub ipfs_path: PathBuf,
    /// The keypair used with libp2p.
    pub keypair: Keypair,
    /// Nodes dialed during startup.
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
    /// Enables mdns for peer discovery when true.
    pub mdns: bool,
    /// Custom Kademlia protocol name.
    pub kad_protocol: Option<String>,
}

impl<Types: IpfsTypes> fmt::Debug for IpfsOptions<Types> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // needed since libp2p::identity::Keypair does not have a Debug impl, and the IpfsOptions
        // is a struct with all public fields, so don't enforce users to use this wrapper.
        fmt.debug_struct("IpfsOptions")
            .field("ipfs_path", &self.ipfs_path)
            .field("bootstrap", &self.bootstrap)
            .field("keypair", &DebuggableKeypair(&self.keypair))
            .field("mdns", &self.mdns)
            .field("kad_protocol", &self.kad_protocol)
            .finish()
    }
}

impl IpfsOptions<TestTypes> {
    /// Creates an inmemory store backed node for tests
    pub fn inmemory_with_generated_keys() -> Self {
        Self {
            _marker: PhantomData,
            ipfs_path: std::env::temp_dir().into(),
            keypair: Keypair::generate_ed25519(),
            mdns: Default::default(),
            bootstrap: Default::default(),
            kad_protocol: Default::default(),
        }
    }
}

/// Workaround for libp2p::identity::Keypair missing a Debug impl, works with references and owned
/// keypairs.
#[derive(Clone)]
struct DebuggableKeypair<I: Borrow<Keypair>>(I);

impl<I: Borrow<Keypair>> fmt::Debug for DebuggableKeypair<I> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.get_ref() {
            Keypair::Ed25519(_) => "Ed25519",
            Keypair::Rsa(_) => "Rsa",
            Keypair::Secp256k1(_) => "Secp256k1",
        };

        write!(fmt, "Keypair::{}", kind)
    }
}

impl<I: Borrow<Keypair>> DebuggableKeypair<I> {
    fn get_ref(&self) -> &Keypair {
        self.0.borrow()
    }
}

impl<Types: IpfsTypes> IpfsOptions<Types> {
    pub fn new(
        ipfs_path: PathBuf,
        keypair: Keypair,
        bootstrap: Vec<(Multiaddr, PeerId)>,
        mdns: bool,
        kad_protocol: Option<String>,
    ) -> Self {
        Self {
            _marker: PhantomData,
            ipfs_path,
            keypair,
            bootstrap,
            mdns,
            kad_protocol,
        }
    }
}

impl<T: IpfsTypes> Default for IpfsOptions<T> {
    /// Create `IpfsOptions` from environment.
    ///
    /// # Panics
    ///
    /// Can panic if two threads call this method at the same time due to race condition on
    /// creating a configuration file under `IPFS_PATH` and other thread failing to read the just
    /// created empty file. Because of this, the implementation has been disabled in tests.
    fn default() -> Self {
        use self::config::ConfigFile;

        if cfg!(test) {
            // making this implementation conditional on `not(test)` results in multiple dead_code
            // lints on config.rs but for rustc 1.42.0 at least having this `cfg!(test)` branch
            // does not result in the same.
            panic!(
                "This implementation must not be invoked when testing as it cannot be safely
                used from multiple threads"
            );
        }

        let ipfs_path = if let Ok(path) = std::env::var("IPFS_PATH") {
            PathBuf::from(path)
        } else {
            let root = if let Some(home) = dirs::home_dir() {
                home
            } else {
                std::env::current_dir().unwrap()
            };
            root.join(".rust-ipfs").into()
        };
        let config_path = dirs::config_dir()
            .unwrap()
            .join("rust-ipfs")
            .join("config.json");
        let config = ConfigFile::new(config_path).unwrap();
        let keypair = config.secio_key_pair();
        let bootstrap = config.bootstrap();

        IpfsOptions {
            _marker: PhantomData,
            ipfs_path,
            keypair,
            bootstrap,
            mdns: true,
            kad_protocol: None,
        }
    }
}

#[derive(Debug)]
pub struct Ipfs(Arc<IpfsInner>);

impl Clone for Ipfs {
    fn clone(&self) -> Self {
        Ipfs(Arc::clone(&self.0))
    }
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
#[derive(Debug)]
pub struct IpfsInner {
    keys: DebuggableKeypair<Keypair>,
    to_task: Sender<IpfsEvent>,
}

type Channel<T> = OneshotSender<Result<T, Error>>;
type FutureSubscription<T, E> = SubscriptionFuture<Result<T, E>>;
type TaskHandle<T, E> = task::JoinHandle<Result<T, E>>;
type SubscriptionList = Vec<(RequestKind, Vec<SubscriptionId>)>;

/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
#[doc(hidden)]
pub enum IpfsEvent {
    /// Connect
    Connect(Multiaddr, OneshotSender<FutureSubscription<(), String>>),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Vec<Multiaddr>>),
    /// Connections
    Connections(Channel<Vec<Connection>>),
    /// Disconnect
    Disconnect(Multiaddr, Channel<()>),
    // TODO
    GetBlock(
        Cid,
        Sender<IpfsEvent>,
        OneshotSender<TaskHandle<Block, Error>>,
    ),
    // TODO
    PutBlock(
        Block,
        OneshotSender<TaskHandle<(Cid, repo::BlockPut), Error>>,
    ),
    // TODO
    ListBlocks(OneshotSender<TaskHandle<Vec<Cid>, Error>>),
    // TODO
    PutDag(Ipld, OneshotSender<TaskHandle<Cid, Error>>),
    // TODO
    GetDag(IpfsPath, OneshotSender<TaskHandle<Ipld, Error>>),
    // TODO
    RemoveBlock(Cid, OneshotSender<TaskHandle<Cid, Error>>),
    // TODO
    PinBlock(Cid, OneshotSender<TaskHandle<(), Error>>),
    // TODO
    UnpinBlock(Cid, OneshotSender<TaskHandle<(), Error>>),
    // TODO
    IsBlockPinned(Cid, OneshotSender<TaskHandle<bool, Error>>),
    // TODO
    ResolveIpns(IpfsPath, OneshotSender<TaskHandle<IpfsPath, Error>>),
    // TODO
    PublishIpns(PeerId, IpfsPath, OneshotSender<TaskHandle<IpfsPath, Error>>),
    // TODO
    CancelIpns(PeerId, OneshotSender<TaskHandle<(), Error>>),
    // TODO
    Add(PathBuf, OneshotSender<TaskHandle<Cid, Error>>),
    // TODO
    Get(IpfsPath, OneshotSender<TaskHandle<unixfs::File, Error>>),
    // TODO
    GetBlockSubscriptions(OneshotSender<SubscriptionList>),
    // TODO
    CancelBlock(Cid),
    /// Request background task to return the listened and external addresses
    GetAddresses(OneshotSender<Vec<Multiaddr>>),
    PubsubSubscribe(String, OneshotSender<Option<SubscriptionStream>>),
    PubsubUnsubscribe(String, OneshotSender<bool>),
    PubsubPublish(String, Vec<u8>, OneshotSender<()>),
    PubsubPeers(Option<String>, OneshotSender<Vec<PeerId>>),
    PubsubSubscribed(OneshotSender<Vec<String>>),
    WantList(Option<PeerId>, OneshotSender<Vec<(Cid, bitswap::Priority)>>),
    BitswapStats(OneshotSender<BitswapStats>),
    AddListeningAddress(Multiaddr, Channel<Multiaddr>),
    RemoveListeningAddress(Multiaddr, Channel<()>),
    Bootstrap(OneshotSender<Result<FutureSubscription<(), String>, Error>>),
    AddPeer(PeerId, Multiaddr),
    GetClosestPeers(PeerId, OneshotSender<FutureSubscription<(), String>>),
    Exit,
}

/// Configured Ipfs instace or value which can be only initialized.
pub struct UninitializedIpfs<Types: IpfsTypes> {
    repo: Option<Repo<Types>>,
    keys: Keypair,
    options: IpfsOptions<Types>,
}

impl<Types: IpfsTypes> UninitializedIpfs<Types> {
    /// Configures a new UninitializedIpfs with from the given options.
    pub async fn new(options: IpfsOptions<Types>) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let repo = create_repo(repo_options);
        let keys = options.keypair.clone();

        UninitializedIpfs {
            repo: Some(repo),
            keys,
            options,
        }
    }

    /// Initialize the ipfs node. The returned `Ipfs` value is cloneable, send and sync, and the
    /// future should be spawned on a executor as soon as possible.
    pub async fn start(mut self) -> Result<(Ipfs, impl std::future::Future<Output = ()>), Error> {
        use futures::stream::StreamExt;

        let repo = Option::take(&mut self.repo).unwrap();
        repo.init().await?;

        let (to_task, receiver) = channel::<IpfsEvent>(1);

        let UninitializedIpfs { keys, .. } = self;

        let ipfs = Ipfs(Arc::new(IpfsInner {
            keys: DebuggableKeypair(keys),
            to_task,
        }));

        let swarm_options = SwarmOptions::<Types>::from(&self.options);
        let swarm = create_swarm(swarm_options, repo).await;

        let fut = IpfsFuture {
            from_facade: receiver.fuse(),
            swarm,
            listening_addresses: HashMap::new(),
        };

        Ok((ipfs, fut))
    }
}

impl std::ops::Deref for Ipfs {
    type Target = IpfsInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ipfs {
    /// Puts a block into the ipfs repo.
    pub async fn put_block(&self, block: Block) -> Result<Cid, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PutBlock(block, tx))
            .await?;

        let cid = rx.await?.await.map(|ret| ret.0);

        // TODO: consider if cancel is applicable in cases where we provide the
        // associated Block ourselves
        //self.cancel_block(&block.cid).await;

        cid
    }

    /// Retrieves a block from the local blockstore, or starts fetching from the network or join an
    /// already started fetch.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        let (tx, rx) = oneshot_channel();
        let cancel_notifier = self.to_task.clone();

        self.to_task
            .clone()
            .send(IpfsEvent::GetBlock(cid.clone(), cancel_notifier, tx))
            .await?;

        rx.await?.await
    }

    /// Remove block from the ipfs repo.
    pub async fn remove_block(&self, cid: Cid) -> Result<Cid, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::RemoveBlock(cid, tx))
            .await?;

        rx.await?.await
    }

    /// Pins a given Cid
    pub async fn pin_block(&self, cid: &Cid) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PinBlock(cid.clone(), tx))
            .await?;

        rx.await?.await
    }

    /// Unpins a given Cid
    pub async fn unpin_block(&self, cid: &Cid) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::UnpinBlock(cid.clone(), tx))
            .await?;

        rx.await?.await
    }

    /// Checks whether a given block is pinned
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::IsBlockPinned(cid.clone(), tx))
            .await?;

        rx.await?.await
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PutDag(ipld, tx))
            .await?;

        rx.await?.await
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::GetDag(path, tx))
            .await?;

        rx.await?.await
    }

    /// Adds a file into the ipfs repo.
    pub async fn add(&self, path: PathBuf) -> Result<Cid, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task.clone().send(IpfsEvent::Add(path, tx)).await?;

        rx.await?.await
    }

    /// Gets a file from the ipfs repo.
    pub async fn get(&self, path: IpfsPath) -> Result<File, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task.clone().send(IpfsEvent::Get(path, tx)).await?;

        rx.await?.await
    }

    /// Creates a stream which will yield the bytes of an UnixFS file from the root Cid, with the
    /// optional file byte range. If the range is specified and is outside of the file, the stream
    /// will end without producing any bytes.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::cat` directly.
    pub async fn cat_unixfs(
        &self,
        cid: Cid,
        range: Option<Range<u64>>,
    ) -> Result<
        impl Stream<Item = Result<Vec<u8>, unixfs::TraversalFailed>> + Send + '_,
        unixfs::TraversalFailed,
    > {
        unixfs::cat(self, cid, range).await
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::ResolveIpns(path.clone(), tx))
            .await?;

        rx.await?.await
    }

    /// Publishes an ipld path.
    pub async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PublishIpns(key.clone(), path.clone(), tx))
            .await?;

        rx.await?.await
    }

    /// Cancel an ipns path.
    pub async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::CancelIpns(key.clone(), tx))
            .await?;

        rx.await?.await
    }

    pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Connect(addr, tx))
            .await?;
        let subscription = rx.await?;
        subscription.await?.map_err(|e| format_err!("{}", e))
    }

    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task.clone().send(IpfsEvent::Addresses(tx)).await?;
        rx.await?
    }

    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;
        rx.await?
    }

    pub async fn peers(&self) -> Result<Vec<Connection>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Connections(tx))
            .await?;
        rx.await?
    }

    pub async fn disconnect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Disconnect(addr, tx))
            .await?;
        rx.await?
    }

    /// Returns the local node public key and the listened and externally visible addresses.
    ///
    /// Public key can be converted to [`PeerId`].
    pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::GetAddresses(tx))
            .await?;
        let addresses = rx.await?;
        Ok((self.keys.get_ref().public(), addresses))
    }

    /// Subscribes to a given topic. Can be done at most once without unsubscribing in the between.
    /// The subscription can be unsubscribed by dropping the stream or calling
    /// [`pubsub_unsubscribe`].
    pub async fn pubsub_subscribe(&self, topic: String) -> Result<SubscriptionStream, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PubsubSubscribe(topic.clone(), tx))
            .await?;

        rx.await?
            .ok_or_else(|| format_err!("already subscribed to {:?}", topic))
    }

    /// Publishes to the topic which may have been subscribed to earlier
    pub async fn pubsub_publish(&self, topic: String, data: Vec<u8>) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PubsubPublish(topic, data, tx))
            .await?;

        Ok(rx.await?)
    }

    /// Returns true if unsubscription was successful
    pub async fn pubsub_unsubscribe(&self, topic: &str) -> Result<bool, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PubsubUnsubscribe(topic.into(), tx))
            .await?;

        Ok(rx.await?)
    }

    /// Returns all known pubsub peers with the optional topic filter
    pub async fn pubsub_peers(&self, topic: Option<String>) -> Result<Vec<PeerId>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PubsubPeers(topic, tx))
            .await?;

        Ok(rx.await?)
    }

    /// Returns all currently subscribed topics
    pub async fn pubsub_subscribed(&self) -> Result<Vec<String>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::PubsubSubscribed(tx))
            .await?;

        Ok(rx.await?)
    }

    pub async fn bitswap_wantlist(
        &self,
        peer: Option<PeerId>,
    ) -> Result<Vec<(Cid, bitswap::Priority)>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::WantList(peer, tx))
            .await?;

        Ok(rx.await?)
    }

    pub async fn refs_local(&self) -> Result<Vec<Cid>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task.clone().send(IpfsEvent::ListBlocks(tx)).await?;

        rx.await?.await
    }

    pub async fn bitswap_stats(&self) -> Result<BitswapStats, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::BitswapStats(tx))
            .await?;

        Ok(rx.await?)
    }

    /// Add a given multiaddr as a listening address. Will fail if the address is unsupported, or
    /// if it is already being listened on. Currently will invoke `Swarm::listen_on` internally,
    /// keep the ListenerId for later `remove_listening_address` use in a HashMap.
    ///
    /// The returned future will resolve on the first bound listening address when this is called
    /// with `/ip4/0.0.0.0/...` or anything similar which will bound through multiple concrete
    /// listening addresses.
    ///
    /// Trying to add an unspecified listening address while any other listening address adding is
    /// in progress will result in error.
    ///
    /// Returns the bound multiaddress, which in the case of original containing an ephemeral port
    /// has now been changed.
    pub async fn add_listening_address(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::AddListeningAddress(addr, tx))
            .await?;

        rx.await?
    }

    /// Stop listening on a previously added listening address. Fails if the address is not being
    /// listened to.
    ///
    /// The removal of all listening addresses added through unspecified addresses is not supported.
    pub async fn remove_listening_address(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::RemoveListeningAddress(addr, tx))
            .await?;

        rx.await?
    }

    /// Exit daemon.
    pub async fn exit_daemon(self) {
        // ignoring the error because it'd mean that the background task had already been dropped
        let _ = self.to_task.clone().try_send(IpfsEvent::Exit);
    }
}

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
struct IpfsFuture<Types: IpfsTypes> {
    swarm: TSwarm<Types>,
    from_facade: Fuse<Receiver<IpfsEvent>>,
    listening_addresses: HashMap<Multiaddr, (ListenerId, Option<Channel<Multiaddr>>)>,
}

impl<TRepoTypes: RepoTypes> IpfsFuture<TRepoTypes> {
    /// Completes the adding of listening address by matching the new listening address `addr` to
    /// the `self.listening_addresses` so that we can detect even the multiaddresses with ephemeral
    /// ports.
    fn complete_listening_address_adding(&mut self, addr: Multiaddr) {
        let maybe_sender = match self.listening_addresses.get_mut(&addr) {
            // matching a non-ephemeral is simpler
            Some((_, maybe_sender)) => maybe_sender.take(),
            None => {
                // try finding an ephemeral binding on the same prefix
                let mut matching_keys = self
                    .listening_addresses
                    .keys()
                    .filter(|right| could_be_bound_from_ephemeral(0, &addr, right))
                    .cloned();

                let first = matching_keys.next();

                if let Some(first) = first {
                    let second = matching_keys.next();

                    match (first, second) {
                        (first, None) => {
                            if let Some((id, maybe_sender)) =
                                self.listening_addresses.remove(&first)
                            {
                                self.listening_addresses.insert(addr.clone(), (id, None));
                                maybe_sender
                            } else {
                                unreachable!("We found a matching ephemeral key already, it must be in the listening_addresses")
                            }
                        }
                        (first, Some(second)) => {
                            // this is more complicated, but we are guarding
                            // against this in the from_facade match below
                            unreachable!(
                                "More than one matching [{}, {}] and {:?} for {}",
                                first,
                                second,
                                matching_keys.collect::<Vec<_>>(),
                                addr
                            );
                        }
                    }
                } else {
                    // this case is hit when user asks for /ip4/0.0.0.0/tcp/0 for example, the
                    // libp2p will bound to multiple addresses but we will not get access in 0.19
                    // to their ListenerIds.

                    let first = self
                        .listening_addresses
                        .iter()
                        .filter(|(addr, _)| starts_unspecified(addr))
                        .filter(|(could_have_ephemeral, _)| {
                            could_be_bound_from_ephemeral(1, &addr, could_have_ephemeral)
                        })
                        // finally we want to make sure we only match on addresses which are yet to
                        // be reported back
                        .filter(|(_, (_, maybe_sender))| maybe_sender.is_some())
                        .map(|(addr, _)| addr.to_owned())
                        .next();

                    if let Some(first) = first {
                        let (id, maybe_sender) = self
                            .listening_addresses
                            .remove(&first)
                            .expect("just filtered this key out");
                        self.listening_addresses.insert(addr.clone(), (id, None));
                        log::trace!("guessing the first match for {} to be {}", first, addr);
                        maybe_sender
                    } else {
                        None
                    }
                }
            }
        };

        if let Some(sender) = maybe_sender {
            let _ = sender.send(Ok(addr));
        }
    }

    fn start_add_listener_address(&mut self, addr: Multiaddr, ret: Channel<Multiaddr>) {
        use libp2p::Swarm;
        use std::collections::hash_map::Entry;

        if starts_unspecified(&addr)
            && self
                .listening_addresses
                .values()
                .filter(|(_, maybe_sender)| maybe_sender.is_some())
                .count()
                > 0
        {
            let _ = ret.send(Err(format_err!("Cannot start listening to unspecified address when there are pending specified addresses awaiting")));
            return;
        }

        match self.listening_addresses.entry(addr) {
            Entry::Occupied(oe) => {
                let _ = ret.send(Err(format_err!("Already adding a possibly ephemeral multiaddr, wait first one to resolve before adding next: {}", oe.key())));
            }
            Entry::Vacant(ve) => match Swarm::listen_on(&mut self.swarm, ve.key().to_owned()) {
                Ok(id) => {
                    ve.insert((id, Some(ret)));
                }
                Err(e) => {
                    let _ = ret.send(Err(Error::from(e)));
                }
            },
        }
    }
}

impl<TRepoTypes: RepoTypes> Future for IpfsFuture<TRepoTypes> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        use libp2p::{swarm::SwarmEvent, Swarm};

        // begin by polling the swarm so that initially it'll first have chance to bind listeners
        // and such.

        let mut done = false;

        loop {
            loop {
                let inner = {
                    let next = self.swarm.next_event();
                    futures::pin_mut!(next);
                    match next.poll(ctx) {
                        Poll::Ready(inner) => inner,
                        Poll::Pending if done => return Poll::Pending,
                        Poll::Pending => break,
                    }
                };
                // as a swarm event was returned, we need to do at least one more round to fully
                // exhaust the swarm before possibly causing the swarm to do more work by popping
                // off the events from Ipfs and ... this looping goes on for a while.
                done = false;
                match inner {
                    SwarmEvent::NewListenAddr(addr) => {
                        self.complete_listening_address_adding(addr);
                    }
                    _ => trace!("{:?}", inner),
                }
            }

            // temporary pinning of the receivers should be safe as we are pinning through the
            // already pinned self. with the receivers we can also safely ignore exhaustion
            // as those are fused.
            loop {
                let inner = match Pin::new(&mut self.from_facade).poll_next(ctx) {
                    Poll::Ready(Some(evt)) => evt,
                    // doing teardown also after the `Ipfs` has been dropped
                    Poll::Ready(None) => IpfsEvent::Exit,
                    Poll::Pending => break,
                };

                match inner {
                    IpfsEvent::GetBlock(cid, ccl_notifier, ret) => {
                        self.swarm.want_block(cid.clone()); // FIXME: only do this if we don't have the block
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move {
                            repo.get_block_with_notifier(&cid, Some(ccl_notifier)).await
                        }))
                        .ok();
                    }
                    IpfsEvent::PutBlock(block, ret) => {
                        self.swarm.provide_block(block.cid.clone());
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.put_block(block).await }))
                            .ok();
                    }
                    IpfsEvent::RemoveBlock(cid, ret) => {
                        self.swarm.stop_providing_block(&cid);
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.remove_block(&cid).await }))
                            .ok();
                    }
                    IpfsEvent::PinBlock(cid, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.pin_block(&cid).await }))
                            .ok();
                    }
                    IpfsEvent::UnpinBlock(cid, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.unpin_block(&cid).await }))
                            .ok();
                    }
                    IpfsEvent::IsBlockPinned(cid, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.is_pinned(&cid).await }))
                            .ok();
                    }
                    IpfsEvent::ListBlocks(ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.list_blocks().await }))
                            .ok();
                    }
                    IpfsEvent::PutDag(ipld, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move {
                            repo.put_dag(ipld, Codec::DagCBOR).await
                        }))
                        .ok();
                    }
                    IpfsEvent::GetDag(path, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.get_dag(path).await }))
                            .ok();
                    }
                    IpfsEvent::ResolveIpns(path, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.resolve_ipns(&path).await }))
                            .ok();
                    }
                    IpfsEvent::PublishIpns(key, path, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(
                            async move { repo.publish_ipns(&key, &path).await },
                        ))
                        .ok();
                    }
                    IpfsEvent::CancelIpns(key, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move { repo.cancel_ipns(&key).await }))
                            .ok();
                    }
                    IpfsEvent::Add(path, ret) => {
                        let repo = self.swarm.repo().clone();

                        ret.send(task::spawn(async move {
                            let file = File::new(path).await?;
                            file.put_unixfs_v1(&repo).await
                        }))
                        .ok();
                    }
                    IpfsEvent::Get(path, ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(task::spawn(async move {
                            File::get_unixfs_v1(&repo, path).await
                        }))
                        .ok();
                    }
                    IpfsEvent::GetBlockSubscriptions(ret) => {
                        let repo = self.swarm.repo().clone();
                        ret.send(
                            repo.subscriptions
                                .subscriptions
                                .lock()
                                .unwrap()
                                .iter()
                                .map(|(kind, subs)| (kind.clone(), subs.keys().copied().collect()))
                                .collect(),
                        )
                        .ok();
                    }
                    IpfsEvent::CancelBlock(cid) => {
                        self.swarm.bitswap().cancel_block(&cid);
                    }
                    IpfsEvent::Connect(addr, ret) => {
                        ret.send(self.swarm.connect(addr)).ok();
                    }
                    IpfsEvent::Addresses(ret) => {
                        let addrs = self.swarm.addrs();
                        ret.send(Ok(addrs)).ok();
                    }
                    IpfsEvent::Listeners(ret) => {
                        let listeners = Swarm::listeners(&self.swarm).cloned().collect();
                        ret.send(Ok(listeners)).ok();
                    }
                    IpfsEvent::Connections(ret) => {
                        let connections = self.swarm.connections();
                        ret.send(Ok(connections.collect())).ok();
                    }
                    IpfsEvent::Disconnect(addr, ret) => {
                        if let Some(disconnector) = self.swarm.disconnect(addr) {
                            disconnector.disconnect(&mut self.swarm);
                        }
                        ret.send(Ok(())).ok();
                    }
                    IpfsEvent::GetAddresses(ret) => {
                        // perhaps this could be moved under `IpfsEvent` or free functions?
                        let mut addresses = Vec::new();
                        addresses.extend(Swarm::listeners(&self.swarm).cloned());
                        addresses.extend(Swarm::external_addresses(&self.swarm).cloned());
                        // ignore error, perhaps caller went away already
                        let _ = ret.send(addresses);
                    }
                    IpfsEvent::PubsubSubscribe(topic, ret) => {
                        let _ = ret.send(self.swarm.pubsub().subscribe(topic));
                    }
                    IpfsEvent::PubsubUnsubscribe(topic, ret) => {
                        let _ = ret.send(self.swarm.pubsub().unsubscribe(topic));
                    }
                    IpfsEvent::PubsubPublish(topic, data, ret) => {
                        self.swarm.pubsub().publish(topic, data);
                        let _ = ret.send(());
                    }
                    IpfsEvent::PubsubPeers(Some(topic), ret) => {
                        let topic = libp2p::floodsub::Topic::new(topic);
                        let _ = ret.send(self.swarm.pubsub().subscribed_peers(&topic));
                    }
                    IpfsEvent::PubsubPeers(None, ret) => {
                        let _ = ret.send(self.swarm.pubsub().known_peers());
                    }
                    IpfsEvent::PubsubSubscribed(ret) => {
                        let _ = ret.send(self.swarm.pubsub().subscribed_topics());
                    }
                    IpfsEvent::WantList(peer, ret) => {
                        let list = if let Some(peer) = peer {
                            self.swarm
                                .bitswap()
                                .peer_wantlist(&peer)
                                .unwrap_or_default()
                        } else {
                            self.swarm.bitswap().local_wantlist()
                        };
                        let _ = ret.send(list);
                    }
                    IpfsEvent::BitswapStats(ret) => {
                        let stats = self.swarm.bitswap().stats();
                        let peers = self.swarm.bitswap().peers();
                        let wantlist = self.swarm.bitswap().local_wantlist();
                        let _ = ret.send((stats, peers, wantlist).into());
                    }
                    IpfsEvent::AddListeningAddress(addr, ret) => {
                        self.start_add_listener_address(addr, ret);
                    }
                    IpfsEvent::RemoveListeningAddress(addr, ret) => {
                        let removed = if let Some((id, _)) = self.listening_addresses.remove(&addr)
                        {
                            Swarm::remove_listener(&mut self.swarm, id).map_err(|_: ()| {
                                format_err!(
                                    "Failed to remove previously added listening address: {}",
                                    addr
                                )
                            })
                        } else {
                            Err(format_err!("Address was not listened to before: {}", addr))
                        };

                        let _ = ret.send(removed);
                    }
                    IpfsEvent::Bootstrap(ret) => {
                        let future = self.swarm.bootstrap();
                        let _ = ret.send(future);
                    }
                    IpfsEvent::AddPeer(peer_id, addr) => {
                        self.swarm.add_peer(peer_id, addr);
                    }
                    IpfsEvent::GetClosestPeers(self_peer, ret) => {
                        let future = self.swarm.get_closest_peers(self_peer);
                        let _ = ret.send(future);
                    }
                    IpfsEvent::Exit => {
                        // FIXME: we could do a proper teardown
                        return Poll::Ready(());
                    }
                }
            }

            done = true;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitswapStats {
    pub blocks_sent: u64,
    pub data_sent: u64,
    pub blocks_received: u64,
    pub data_received: u64,
    pub dup_blks_received: u64,
    pub dup_data_received: u64,
    pub peers: Vec<PeerId>,
    pub wantlist: Vec<(Cid, bitswap::Priority)>,
}

impl From<(bitswap::Stats, Vec<PeerId>, Vec<(Cid, bitswap::Priority)>)> for BitswapStats {
    fn from(
        (stats, peers, wantlist): (bitswap::Stats, Vec<PeerId>, Vec<(Cid, bitswap::Priority)>),
    ) -> Self {
        BitswapStats {
            blocks_sent: stats.sent_blocks.load(Ordering::Relaxed),
            data_sent: stats.sent_data.load(Ordering::Relaxed),
            blocks_received: stats.received_blocks.load(Ordering::Relaxed),
            data_received: stats.received_data.load(Ordering::Relaxed),
            dup_blks_received: stats.duplicate_blocks.load(Ordering::Relaxed),
            dup_data_received: stats.duplicate_data.load(Ordering::Relaxed),
            peers,
            wantlist,
        }
    }
}

#[doc(hidden)]
pub use node::Node;

mod node {
    use super::*;

    /// Node encapsulates everything to setup a testing instance so that multi-node tests become
    /// easier.
    pub struct Node {
        ipfs: Ipfs,
        background_task: Option<task::JoinHandle<()>>,
    }

    impl Node {
        pub async fn new() -> Self {
            let opts = IpfsOptions::inmemory_with_generated_keys();
            Node::with_options(opts).await
        }

        pub async fn with_options(opts: IpfsOptions<TestTypes>) -> Self {
            let (ipfs, fut) = UninitializedIpfs::new(opts).await.start().await.unwrap();

            let background_task = Some(task::spawn(fut));

            Node {
                ipfs,
                background_task,
            }
        }

        pub async fn get_subscriptions(&self) -> Result<SubscriptionList, Error> {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetBlockSubscriptions(tx))
                .await?;

            Ok(rx.await?)
        }

        pub async fn get_closest_peers(&self) -> Result<(), Error> {
            let self_peer = PeerId::from_public_key(self.identity().await?.0);
            let (tx, rx) = oneshot_channel::<FutureSubscription<(), String>>();

            self.to_task
                .clone()
                .send(IpfsEvent::GetClosestPeers(self_peer, tx))
                .await?;

            rx.await?.await?.map_err(|e| anyhow!(e))
        }

        /// Initiate a query for random key to discover peers.
        pub async fn bootstrap(&self) -> Result<(), Error> {
            let (tx, rx) = oneshot_channel::<Result<FutureSubscription<(), String>, Error>>();

            self.to_task.clone().send(IpfsEvent::Bootstrap(tx)).await?;

            rx.await??.await?.map_err(|e| anyhow!(e))
        }

        /// Add a known peer to the DHT.
        pub async fn add_peer(&self, peer_id: PeerId, addr: Multiaddr) -> Result<(), Error> {
            self.to_task
                .clone()
                .send(IpfsEvent::AddPeer(peer_id, addr))
                .await?;

            Ok(())
        }

        pub async fn shutdown(mut self) {
            self.ipfs.exit_daemon().await;
            if let Some(task) = std::mem::take(&mut self.background_task) {
                task.await;
            }
        }
    }

    impl Clone for Node {
        fn clone(&self) -> Self {
            Node {
                ipfs: self.ipfs.clone(),
                background_task: None,
            }
        }
    }

    impl std::ops::Deref for Node {
        type Target = Ipfs;

        fn deref(&self) -> &Self::Target {
            &self.ipfs
        }
    }

    impl std::ops::DerefMut for Node {
        fn deref_mut(&mut self) -> &mut <Self as std::ops::Deref>::Target {
            &mut self.ipfs
        }
    }
}

// Checks if the multiaddr starts with ip4 or ip6 unspecified address, like 0.0.0.0
fn starts_unspecified(addr: &Multiaddr) -> bool {
    use libp2p::core::multiaddr::Protocol;

    match addr.iter().next() {
        Some(Protocol::Ip4(ip4)) if ip4.is_unspecified() => true,
        Some(Protocol::Ip6(ip6)) if ip6.is_unspecified() => true,
        _ => false,
    }
}

fn could_be_bound_from_ephemeral(
    skip: usize,
    bound: &Multiaddr,
    may_have_ephemeral: &Multiaddr,
) -> bool {
    use libp2p::core::multiaddr::Protocol;

    if bound.len() != may_have_ephemeral.len() {
        // no zip_longest in std
        false
    } else {
        // this is could be wrong at least in the future; /p2p/peerid is not a
        // valid suffix but I could imagine some kind of ws or webrtc could
        // give us issues in the long future?
        bound
            .iter()
            .skip(skip)
            .zip(may_have_ephemeral.iter().skip(skip))
            .all(|(left, right)| match (right, left) {
                (Protocol::Tcp(0), Protocol::Tcp(x))
                | (Protocol::Udp(0), Protocol::Udp(x))
                | (Protocol::Sctp(0), Protocol::Sctp(x)) => {
                    assert_ne!(x, 0, "cannot have bound to port 0");
                    true
                }
                (Protocol::Memory(0), Protocol::Memory(x)) => {
                    assert_ne!(x, 0, "cannot have bound to port 0");
                    true
                }
                (right, left) => right == left,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::ipld;
    use libp2p::build_multiaddr;
    use multihash::Sha2_256;

    pub async fn create_mock_ipfs() -> Ipfs {
        let options = IpfsOptions::inmemory_with_generated_keys();
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        ipfs
    }

    #[test]
    fn unspecified_multiaddrs() {
        assert!(starts_unspecified(&build_multiaddr!(
            Ip4([0, 0, 0, 0]),
            Tcp(1u16)
        )));
        assert!(starts_unspecified(&build_multiaddr!(
            Ip6([0, 0, 0, 0, 0, 0, 0, 0]),
            Tcp(1u16)
        )));
    }

    #[test]
    fn localhost_multiaddrs_are_not_unspecified() {
        assert!(!starts_unspecified(&build_multiaddr!(
            Ip4([127, 0, 0, 1]),
            Tcp(1u16)
        )));
        assert!(!starts_unspecified(&build_multiaddr!(
            Ip6([0, 0, 0, 0, 0, 0, 0, 1]),
            Tcp(1u16)
        )));
    }

    #[test]
    fn bound_ephemerals() {
        assert!(could_be_bound_from_ephemeral(
            0,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16))
        ));

        assert!(!could_be_bound_from_ephemeral(
            0,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));

        assert!(!could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(44444u16))
        ));
    }

    #[async_std::test]
    async fn test_put_and_get_block() {
        let ipfs = create_mock_ipfs().await;

        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);

        ipfs.exit_daemon().await;
    }

    #[async_std::test]
    async fn test_put_and_get_dag() {
        let ipfs = create_mock_ipfs().await;

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);

        ipfs.exit_daemon().await;
    }

    #[async_std::test]
    async fn test_pin_and_unpin() {
        let ipfs = create_mock_ipfs().await;

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();

        ipfs.pin_block(&cid).await.unwrap();
        assert!(ipfs.is_pinned(&cid).await.unwrap());
        ipfs.unpin_block(&cid).await.unwrap();
        assert!(!ipfs.is_pinned(&cid).await.unwrap());

        ipfs.exit_daemon().await;
    }

    #[test]
    #[should_panic]
    fn default_ipfs_options_disabled_when_testing() {
        IpfsOptions::<TestTypes>::default();
    }
}
