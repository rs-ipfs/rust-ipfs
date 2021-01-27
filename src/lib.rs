//! IPFS node implementation
//!
//! [Ipfs](https://ipfs.io) is a peer-to-peer system with content addressed functionality. The main
//! entry point for users of this crate is the [`Ipfs`] facade, which allows access to most of the
//! implemented functionality.
//!
//! This crate passes a lot of the [interface-ipfs-core] test suite; most of that functionality is
//! in `ipfs-http` crate. The crate has some interoperability with the [go-ipfs] and [js-ipfs]
//! implementations.
//!
//! `ipfs` is an early alpha level crate: APIs and their implementation are subject to change in
//! any upcoming release at least for now. The aim of the crate is to become a library-first
//! production ready implementation of an Ipfs node.
//!
//! [interface-ipfs-core]: https://www.npmjs.com/package/interface-ipfs-core
//! [go-ipfs]: https://github.com/ipfs/go-ipfs/
//! [js-ipfs]: https://github.com/ipfs/js-ipfs/
// We are not done yet, but uncommenting this makes it easier to hunt down for missing docs.
//#![deny(missing_docs)]
//
// This isn't recognized in stable yet, but we should disregard any nags on these to keep making
// the docs better.
//#![allow(private_intra_doc_links)]

pub mod config;
pub mod dag;
pub mod error;
#[macro_use]
pub mod ipld;
pub mod ipns;
pub mod p2p;
pub mod path;
pub mod refs;
pub mod repo;
mod subscription;
pub mod unixfs;

#[macro_use]
extern crate tracing;

use anyhow::{anyhow, format_err};
use cid::Codec;
use either::Either;
use futures::{
    channel::{
        mpsc::{channel, Receiver, Sender},
        oneshot::{channel as oneshot_channel, Sender as OneshotSender},
    },
    sink::SinkExt,
    stream::{Fuse, Stream},
};
use libp2p::swarm::NetworkBehaviour;
use tracing::Span;
use tracing_futures::Instrument;

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    env, fmt,
    future::Future,
    ops::{Deref, DerefMut, Range},
    path::PathBuf,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use self::{
    dag::IpldDag,
    ipns::Ipns,
    p2p::{
        addr::{could_be_bound_from_ephemeral, starts_unspecified},
        create_swarm, SwarmOptions, TSwarm,
    },
    repo::{create_repo, Repo, RepoEvent, RepoOptions},
    subscription::SubscriptionFuture,
};

pub use self::{
    error::Error,
    ipld::Ipld,
    p2p::{
        pubsub::{PubsubMessage, SubscriptionStream},
        Connection, KadResult, MultiaddrWithPeerId, MultiaddrWithoutPeerId,
    },
    path::IpfsPath,
    repo::{PinKind, PinMode, RepoTypes},
};
pub use cid::Cid;
pub use ipfs_bitswap::Block;
pub use libp2p::{
    core::{
        connection::ListenerId, multiaddr::multiaddr, multiaddr::Protocol, Multiaddr, PeerId,
        PublicKey,
    },
    identity::Keypair,
    kad::{record::Key, Quorum},
};

/// Represents the configuration of the Ipfs node, its backing blockstore and datastore.
pub trait IpfsTypes: RepoTypes {}
impl<T: RepoTypes> IpfsTypes for T {}

/// Default node configuration, currently with persistent block store and data store for pins.
#[derive(Debug)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    #[cfg(feature = "sled_data_store")]
    type TDataStore = repo::kv::KvDataStore;
    #[cfg(not(feature = "sled_data_store"))]
    type TDataStore = repo::fs::FsDataStore;
    type TLock = repo::fs::FsLock;
}

/// In-memory testing configuration used in tests.
#[derive(Debug)]
pub struct TestTypes;
impl RepoTypes for TestTypes {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
    type TLock = repo::mem::MemLock;
}

/// Ipfs node options used to configure the node to be created with [`UninitializedIpfs`].
#[derive(Clone)]
pub struct IpfsOptions {
    /// The path of the ipfs repo (blockstore and datastore).
    ///
    /// This is always required but can be any path with in-memory backends. The filesystem backend
    /// creates a directory structure alike but not compatible to other ipfs implementations.
    ///
    /// # Incompatiblity and interop warning
    ///
    /// It is **not** recommended to set this to IPFS_PATH without first at least backing up your
    /// existing repository.
    pub ipfs_path: PathBuf,

    /// The keypair used with libp2p, the identity of the node.
    pub keypair: Keypair,

    /// Nodes used as bootstrap peers.
    pub bootstrap: Vec<(Multiaddr, PeerId)>,

    /// Enables mdns for peer discovery and announcement when true.
    pub mdns: bool,

    /// Custom Kademlia protocol name. When set to `None`, the global DHT name is used instead of
    /// the LAN dht name.
    ///
    /// The name given here is passed to [`libp2p_kad::KademliaConfig::set_protocol_name`].
    ///
    /// [`libp2p_kad::KademliaConfig::set_protocol_name`]: https://docs.rs/libp2p-kad/*/libp2p_kad/struct.KademliaConfig.html##method.set_protocol_name
    pub kad_protocol: Option<String>,

    /// Bound listening addresses; by default the node will not listen on any address.
    pub listening_addrs: Vec<Multiaddr>,

    /// The span for tracing purposes, `None` value is converted to `tracing::trace_span!("ipfs")`.
    ///
    /// All futures returned by `Ipfs`, background task actions and swarm actions are instrumented
    /// with this span or spans referring to this as their parent. Setting this other than `None`
    /// default is useful when running multiple nodes.
    pub span: Option<Span>,
}

impl fmt::Debug for IpfsOptions {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // needed since libp2p::identity::Keypair does not have a Debug impl, and the IpfsOptions
        // is a struct with all public fields, don't enforce users to use this wrapper.
        fmt.debug_struct("IpfsOptions")
            .field("ipfs_path", &self.ipfs_path)
            .field("bootstrap", &self.bootstrap)
            .field("keypair", &DebuggableKeypair(&self.keypair))
            .field("mdns", &self.mdns)
            .field("kad_protocol", &self.kad_protocol)
            .field("listening_addrs", &self.listening_addrs)
            .field("span", &self.span)
            .finish()
    }
}

impl IpfsOptions {
    /// Creates an in-memory store backed configuration useful for any testing purposes.
    ///
    /// Also used from examples.
    pub fn inmemory_with_generated_keys() -> Self {
        Self {
            ipfs_path: env::temp_dir(),
            keypair: Keypair::generate_ed25519(),
            mdns: Default::default(),
            bootstrap: Default::default(),
            // default to lan kad for go-ipfs use in tests
            kad_protocol: Some("/ipfs/lan/kad/1.0.0".to_owned()),
            listening_addrs: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            span: None,
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

/// The facade for the Ipfs node.
///
/// The facade has most of the functionality either directly as a method or the functionality can
/// be implemented using the provided methods. For more information, see examples or the HTTP
/// endpoint implementations in `ipfs-http`.
///
/// The facade is created through [`UninitializedIpfs`] which is configured with [`IpfsOptions`].
#[derive(Debug)]
pub struct Ipfs<Types: IpfsTypes> {
    span: Span,
    repo: Arc<Repo<Types>>,
    keys: DebuggableKeypair<Keypair>,
    to_task: Sender<IpfsEvent>,
}

impl<Types: IpfsTypes> Clone for Ipfs<Types> {
    fn clone(&self) -> Self {
        Ipfs {
            span: self.span.clone(),
            repo: Arc::clone(&self.repo),
            keys: self.keys.clone(),
            to_task: self.to_task.clone(),
        }
    }
}

type Channel<T> = OneshotSender<Result<T, Error>>;

/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
enum IpfsEvent {
    /// Connect
    Connect(
        MultiaddrWithPeerId,
        OneshotSender<Option<SubscriptionFuture<(), String>>>,
    ),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Vec<Multiaddr>>),
    /// Connections
    Connections(Channel<Vec<Connection>>),
    /// Disconnect
    Disconnect(MultiaddrWithPeerId, Channel<()>),
    /// Request background task to return the listened and external addresses
    GetAddresses(OneshotSender<Vec<Multiaddr>>),
    PubsubSubscribe(String, OneshotSender<Option<SubscriptionStream>>),
    PubsubUnsubscribe(String, OneshotSender<bool>),
    PubsubPublish(String, Vec<u8>, OneshotSender<()>),
    PubsubPeers(Option<String>, OneshotSender<Vec<PeerId>>),
    PubsubSubscribed(OneshotSender<Vec<String>>),
    WantList(
        Option<PeerId>,
        OneshotSender<Vec<(Cid, ipfs_bitswap::Priority)>>,
    ),
    BitswapStats(OneshotSender<BitswapStats>),
    AddListeningAddress(Multiaddr, Channel<Multiaddr>),
    RemoveListeningAddress(Multiaddr, Channel<()>),
    Bootstrap(Channel<SubscriptionFuture<KadResult, String>>),
    AddPeer(PeerId, Multiaddr),
    GetClosestPeers(PeerId, OneshotSender<SubscriptionFuture<KadResult, String>>),
    GetBitswapPeers(OneshotSender<Vec<PeerId>>),
    FindPeer(
        PeerId,
        bool,
        OneshotSender<Either<Vec<Multiaddr>, SubscriptionFuture<KadResult, String>>>,
    ),
    GetProviders(Cid, OneshotSender<SubscriptionFuture<KadResult, String>>),
    Provide(Cid, Channel<SubscriptionFuture<KadResult, String>>),
    DhtGet(
        Key,
        Quorum,
        OneshotSender<SubscriptionFuture<KadResult, String>>,
    ),
    DhtPut(
        Key,
        Vec<u8>,
        Quorum,
        Channel<SubscriptionFuture<KadResult, String>>,
    ),
    GetBootstrappers(OneshotSender<Vec<Multiaddr>>),
    AddBootstrapper(MultiaddrWithPeerId, Channel<Multiaddr>),
    RemoveBootstrapper(MultiaddrWithPeerId, Channel<Multiaddr>),
    ClearBootstrappers(OneshotSender<Vec<Multiaddr>>),
    RestoreBootstrappers(Channel<Vec<Multiaddr>>),
    Exit,
}

/// Configured Ipfs which can only be started.
pub struct UninitializedIpfs<Types: IpfsTypes> {
    repo: Arc<Repo<Types>>,
    keys: Keypair,
    options: IpfsOptions,
    repo_events: Receiver<RepoEvent>,
}

impl<Types: IpfsTypes> UninitializedIpfs<Types> {
    /// Configures a new UninitializedIpfs with from the given options and optionally a span.
    /// If the span is not given, it is defaulted to `tracing::trace_span!("ipfs")`.
    ///
    /// The span is attached to all operations called on the later created `Ipfs` along with all
    /// operations done in the background task as well as tasks spawned by the underlying
    /// `libp2p::Swarm`.
    pub fn new(options: IpfsOptions) -> Self {
        let repo_options = RepoOptions::from(&options);
        let (repo, repo_events) = create_repo(repo_options);
        let keys = options.keypair.clone();

        UninitializedIpfs {
            repo: Arc::new(repo),
            keys,
            options,
            repo_events,
        }
    }

    /// Initialize the ipfs node. The returned `Ipfs` value is cloneable, send and sync, and the
    /// future should be spawned on a executor as soon as possible.
    ///
    /// The future returned from this method should not need
    /// (instrumenting)[`tracing_futures::Instrument::instrument`] as the [`IpfsOptions::span`]
    /// will be used as parent span for all of the awaited and created futures.
    pub async fn start(self) -> Result<(Ipfs<Types>, impl Future<Output = ()>), Error> {
        use futures::stream::StreamExt;

        let UninitializedIpfs {
            repo,
            keys,
            repo_events,
            mut options,
        } = self;

        let root_span = options
            .span
            .take()
            // not sure what would be the best practice with tracing and spans
            .unwrap_or_else(|| tracing::trace_span!(parent: &Span::current(), "ipfs"));

        // the "current" span which is not entered but the awaited futures are instrumented with it
        let init_span = tracing::trace_span!(parent: &root_span, "init");

        // stored in the Ipfs, instrumenting every method call
        let facade_span = tracing::trace_span!("facade");

        // stored in the executor given to libp2p, used to spawn at least the connections,
        // instrumenting each of those.
        let exec_span = tracing::trace_span!(parent: &root_span, "exec");

        // instruments the IpfsFuture, the background task.
        let swarm_span = tracing::trace_span!(parent: &root_span, "swarm");

        repo.init().instrument(init_span.clone()).await?;

        let (to_task, receiver) = channel::<IpfsEvent>(1);

        let ipfs = Ipfs {
            span: facade_span,
            repo: repo.clone(),
            keys: DebuggableKeypair(keys),
            to_task,
        };

        // FIXME: mutating options above is an unfortunate side-effect of this call, which could be
        // reordered for less error prone code.
        let swarm_options = SwarmOptions::from(&options);
        let swarm = create_swarm(swarm_options, exec_span, repo)
            .instrument(tracing::trace_span!(parent: &init_span, "swarm"))
            .await?;

        let IpfsOptions {
            listening_addrs, ..
        } = options;

        let mut fut = IpfsFuture {
            repo_events: repo_events.fuse(),
            from_facade: receiver.fuse(),
            swarm,
            listening_addresses: HashMap::with_capacity(listening_addrs.len()),
        };

        for addr in listening_addrs.into_iter() {
            fut.start_add_listener_address(addr, None);
        }

        Ok((ipfs, fut.instrument(swarm_span)))
    }
}

impl<Types: IpfsTypes> Ipfs<Types> {
    /// Return an [`IpldDag`] for DAG operations
    pub fn dag(&self) -> IpldDag<Types> {
        IpldDag::new(self.clone())
    }

    fn ipns(&self) -> Ipns<Types> {
        Ipns::new(self.clone())
    }

    /// Puts a block into the ipfs repo.
    ///
    /// # Forget safety
    ///
    /// Forgetting the returned future will not result in memory unsafety, but it can
    /// deadlock other tasks.
    pub async fn put_block(&self, block: Block) -> Result<Cid, Error> {
        self.repo
            .put_block(block)
            .instrument(self.span.clone())
            .await
            .map(|(cid, _put_status)| cid)
    }

    /// Retrieves a block from the local blockstore, or starts fetching from the network or join an
    /// already started fetch.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        self.repo.get_block(cid).instrument(self.span.clone()).await
    }

    /// Remove block from the ipfs repo. A pinned block cannot be removed.
    pub async fn remove_block(&self, cid: Cid) -> Result<Cid, Error> {
        self.repo
            .remove_block(&cid)
            .instrument(self.span.clone())
            .await
    }

    /// Pins a given Cid recursively or directly (non-recursively).
    ///
    /// Pins on a block are additive in sense that a previously directly (non-recursively) pinned
    /// can be made recursive, but removing the recursive pin on the block removes also the direct
    /// pin as well.
    ///
    /// Pinning a Cid recursively (for supported dag-protobuf and dag-cbor) will walk its
    /// references and pin the references indirectly. When a Cid is pinned indirectly it will keep
    /// its previous direct or recursive pin and be indirect in addition.
    ///
    /// Recursively pinned Cids cannot be re-pinned non-recursively but non-recursively pinned Cids
    /// can be "upgraded to" being recursively pinned.
    ///
    /// # Crash unsafety
    ///
    /// If a recursive `insert_pin` operation is interrupted because of a crash or the crash
    /// prevents from synchronizing the data store to disk, this will leave the system in an inconsistent
    /// state. The remedy is to re-pin recursive pins.
    pub async fn insert_pin(&self, cid: &Cid, recursive: bool) -> Result<(), Error> {
        use futures::stream::{StreamExt, TryStreamExt};
        let span = debug_span!(parent: &self.span, "insert_pin", cid = %cid, recursive);
        let refs_span = debug_span!(parent: &span, "insert_pin refs");

        async move {
            // this needs to download everything but /pin/ls does not
            let Block { data, .. } = self.repo.get_block(cid).await?;

            if !recursive {
                self.repo.insert_direct_pin(cid).await
            } else {
                let ipld = crate::ipld::decode_ipld(&cid, &data)?;

                let st = crate::refs::IpldRefs::default()
                    .with_only_unique()
                    .refs_of_resolved(self, vec![(cid.clone(), ipld.clone())].into_iter())
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .instrument(refs_span)
                    .boxed();

                self.repo.insert_recursive_pin(cid, st).await
            }
        }
        .instrument(span)
        .await
    }

    /// Unpins a given Cid recursively or only directly.
    ///
    /// Recursively unpinning a previously only directly pinned Cid will remove the direct pin.
    ///
    /// Unpinning an indirectly pinned Cid is not possible other than through its recursively
    /// pinned tree roots.
    pub async fn remove_pin(&self, cid: &Cid, recursive: bool) -> Result<(), Error> {
        use futures::stream::{StreamExt, TryStreamExt};
        let span = debug_span!(parent: &self.span, "remove_pin", cid = %cid, recursive);
        async move {
            if !recursive {
                self.repo.remove_direct_pin(cid).await
            } else {
                // start walking refs of the root after loading it

                let Block { data, .. } = match self.repo.get_block_now(&cid).await? {
                    Some(b) => b,
                    None => {
                        return Err(anyhow::anyhow!("pinned root not found: {}", cid));
                    }
                };

                let ipld = crate::ipld::decode_ipld(&cid, &data)?;
                let st = crate::refs::IpldRefs::default()
                    .with_only_unique()
                    .with_existing_blocks()
                    .refs_of_resolved(
                        self.to_owned(),
                        vec![(cid.clone(), ipld.clone())].into_iter(),
                    )
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .boxed();

                self.repo.remove_recursive_pin(cid, st).await
            }
        }
        .instrument(span)
        .await
    }

    /// Checks whether a given block is pinned.
    ///
    /// Returns true if the block is pinned, false if not. See Crash unsafety notes for the false
    /// response.
    ///
    /// # Crash unsafety
    ///
    /// Cannot currently detect partially written recursive pins. Those can happen if
    /// `Ipfs::insert_pin(cid, true)` is interrupted by a crash for example.
    ///
    /// Works correctly only under no-crash situations. Workaround for hitting a crash is to re-pin
    /// any existing recursive pins.
    ///
    // TODO: This operation could be provided as a `Ipfs::fix_pins()`.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let span = debug_span!(parent: &self.span, "is_pinned", cid = %cid);
        self.repo.is_pinned(cid).instrument(span).await
    }

    /// Lists all pins, or the specific kind thereof.
    ///
    /// # Crash unsafety
    ///
    /// Does not currently recover from partial recursive pin insertions.
    pub async fn list_pins(
        &self,
        filter: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let span = debug_span!(parent: &self.span, "list_pins", ?filter);
        self.repo.list_pins(filter).instrument(span).await
    }

    /// Read specific pins. When `requirement` is `Some`, all pins are required to be of the given
    /// [`PinMode`].
    ///
    /// # Crash unsafety
    ///
    /// Does not currently recover from partial recursive pin insertions.
    pub async fn query_pins(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let span = debug_span!(parent: &self.span, "query_pins", ids = cids.len(), ?requirement);
        self.repo
            .query_pins(cids, requirement)
            .instrument(span)
            .await
    }

    /// Puts an ipld node into the ipfs repo using `dag-cbor` codec and Sha2_256 hash.
    ///
    /// Returns Cid version 1 for the document
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        self.dag()
            .put(ipld, Codec::DagCBOR)
            .instrument(self.span.clone())
            .await
    }

    /// Gets an ipld node from the ipfs, fetching the block if necessary.
    ///
    /// See [`IpldDag::get`] for more information.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        self.dag()
            .get(path)
            .instrument(self.span.clone())
            .await
            .map_err(Error::new)
    }

    /// Creates a stream which will yield the bytes of an UnixFS file from the root Cid, with the
    /// optional file byte range. If the range is specified and is outside of the file, the stream
    /// will end without producing any bytes.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::cat` directly.
    pub async fn cat_unixfs(
        &self,
        starting_point: impl Into<unixfs::StartingPoint>,
        range: Option<Range<u64>>,
    ) -> Result<
        impl Stream<Item = Result<Vec<u8>, unixfs::TraversalFailed>> + Send + '_,
        unixfs::TraversalFailed,
    > {
        // convert early not to worry about the lifetime of parameter
        let starting_point = starting_point.into();
        unixfs::cat(self, starting_point, range)
            .instrument(self.span.clone())
            .await
    }

    /// Resolves a ipns path to an ipld path; currently only supports dnslink resolution.
    pub async fn resolve_ipns(&self, path: &IpfsPath, recursive: bool) -> Result<IpfsPath, Error> {
        async move {
            let ipns = self.ipns();
            let mut resolved = ipns.resolve(path).await;

            if recursive {
                let mut seen = HashSet::with_capacity(1);
                while let Ok(ref res) = resolved {
                    if !seen.insert(res.clone()) {
                        break;
                    }
                    resolved = ipns.resolve(&res).await;
                }

                resolved
            } else {
                resolved
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Connects to the peer at the given Multiaddress.
    ///
    /// Accepts only multiaddresses with the PeerId to authenticate the connection.
    ///
    /// Returns a future which will complete when the connection has been successfully made or
    /// failed for whatever reason. It is possible for this method to return an error, while ending
    /// up being connected to the peer by the means of another connection.
    pub async fn connect(&self, target: MultiaddrWithPeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Connect(target, tx))
                .await?;
            let subscription = rx.await?;

            if let Some(future) = subscription {
                future.await.map_err(|e| anyhow!(e))
            } else {
                futures::future::ready(Err(anyhow!("Duplicate connection attempt"))).await
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns known peer addresses
    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Addresses(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns local listening addresses
    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the connected peers
    pub async fn peers(&self) -> Result<Vec<Connection>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Connections(tx))
                .await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Disconnects a given peer.
    ///
    /// At the moment the peer is disconnected by temporarily banning the peer and unbanning it
    /// right after. This should always disconnect all connections to the peer.
    ///
    /// Note: this is rarely needed in pratice as connections will time out if left unused.
    pub async fn disconnect(&self, target: MultiaddrWithPeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Disconnect(target, tx))
                .await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the local node public key and the listened and externally visible addresses.
    /// The addresses are suffixed with the P2p protocol containing the node's PeerId.
    ///
    /// Public key can be converted to [`PeerId`].
    pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetAddresses(tx))
                .await?;
            let mut addresses = rx.await?;
            let public_key = self.keys.get_ref().public();
            let peer_id = public_key.clone().into_peer_id();

            for addr in &mut addresses {
                addr.push(Protocol::P2p(peer_id.clone().into()))
            }

            Ok((public_key, addresses))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Subscribes to a given topic. Can be done at most once without unsubscribing in the between.
    /// The subscription can be unsubscribed by dropping the stream or calling
    /// [`Ipfs::pubsub_unsubscribe`].
    pub async fn pubsub_subscribe(&self, topic: String) -> Result<SubscriptionStream, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubSubscribe(topic.clone(), tx))
                .await?;

            rx.await?
                .ok_or_else(|| format_err!("already subscribed to {:?}", topic))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Publishes to the topic which may have been subscribed to earlier
    pub async fn pubsub_publish(&self, topic: String, data: Vec<u8>) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPublish(topic, data, tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Forcibly unsubscribes a previously made [`SubscriptionStream`], which could also be
    /// unsubscribed by dropping the stream.
    ///
    /// Returns true if unsubscription was successful
    pub async fn pubsub_unsubscribe(&self, topic: &str) -> Result<bool, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubUnsubscribe(topic.into(), tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns all known pubsub peers with the optional topic filter
    pub async fn pubsub_peers(&self, topic: Option<String>) -> Result<Vec<PeerId>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPeers(topic, tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns all currently subscribed topics
    pub async fn pubsub_subscribed(&self) -> Result<Vec<String>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubSubscribed(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the known wantlist for the local node when the `peer` is `None` or the wantlist of the given `peer`
    pub async fn bitswap_wantlist(
        &self,
        peer: Option<PeerId>,
    ) -> Result<Vec<(Cid, ipfs_bitswap::Priority)>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::WantList(peer, tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns a list of local blocks
    ///
    /// This implementation is subject to change into a stream, which might only include the pinned
    /// blocks.
    pub async fn refs_local(&self) -> Result<Vec<Cid>, Error> {
        self.repo.list_blocks().instrument(self.span.clone()).await
    }

    /// Returns the accumulated bitswap stats
    pub async fn bitswap_stats(&self) -> Result<BitswapStats, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::BitswapStats(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
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
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddListeningAddress(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stop listening on a previously added listening address. Fails if the address is not being
    /// listened to.
    ///
    /// The removal of all listening addresses added through unspecified addresses is not supported.
    pub async fn remove_listening_address(&self, addr: Multiaddr) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RemoveListeningAddress(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Obtain the addresses associated with the given `PeerId`; they are first searched for locally
    /// and the DHT is used as a fallback: a `Kademlia::get_closest_peers(peer_id)` query is run and
    /// when it's finished, the newly added DHT records are checked for the existence of the desired
    /// `peer_id` and if it's there, the list of its known addresses is returned.
    pub async fn find_peer(&self, peer_id: PeerId) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::FindPeer(peer_id, false, tx))
                .await?;

            match rx.await? {
                Either::Left(addrs) if !addrs.is_empty() => Ok(addrs),
                Either::Left(_) => unreachable!(),
                Either::Right(future) => {
                    future.await?;

                    let (tx, rx) = oneshot_channel();

                    self.to_task
                        .clone()
                        .send(IpfsEvent::FindPeer(peer_id, true, tx))
                        .await?;

                    match rx.await? {
                        Either::Left(addrs) if !addrs.is_empty() => Ok(addrs),
                        _ => Err(anyhow!("couldn't find peer {}", peer_id)),
                    }
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Performs a DHT lookup for providers of a value to the given key.
    ///
    /// Returns a list of peers found providing the Cid.
    pub async fn get_providers(&self, cid: Cid) -> Result<Vec<PeerId>, Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetProviders(cid, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result {
            Ok(KadResult::Peers(providers)) => Ok(providers),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Establishes the node as a provider of a block with the given Cid: it publishes a provider
    /// record with the given key (Cid) and the node's PeerId to the peers closest to the key. The
    /// publication of provider records is periodically repeated as per the interval specified in
    /// `libp2p`'s  `KademliaConfig`.
    pub async fn provide(&self, cid: Cid) -> Result<(), Error> {
        // don't provide things we don't actually have
        if self.repo.get_block_now(&cid).await?.is_none() {
            return Err(anyhow!(
                "Error: block {} not found locally, cannot provide",
                cid
            ));
        }

        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::Provide(cid, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result {
            Ok(KadResult::Complete) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Returns a list of peers closest to the given `PeerId`, as suggested by the DHT. The
    /// node must have at least one known peer in its routing table in order for the query
    /// to return any values.
    pub async fn get_closest_peers(&self, peer_id: PeerId) -> Result<Vec<PeerId>, Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetClosestPeers(peer_id, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result {
            Ok(KadResult::Peers(closest)) => Ok(closest),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Attempts to look a key up in the DHT and returns the values found in the records
    /// containing that key.
    pub async fn dht_get<T: Into<Key>>(
        &self,
        key: T,
        quorum: Quorum,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtGet(key.into(), quorum, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result {
            Ok(KadResult::Records(recs)) => {
                let values = recs.into_iter().map(|rec| rec.value).collect();
                Ok(values)
            }
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Stores the given key + value record locally and replicates it in the DHT. It doesn't
    /// expire locally and is periodically replicated in the DHT, as per the `KademliaConfig`
    /// setup.
    pub async fn dht_put<T: Into<Key>>(
        &self,
        key: T,
        value: Vec<u8>,
        quorum: Quorum,
    ) -> Result<(), Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtPut(key.into(), value, quorum, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await??
        .await;

        match kad_result {
            Ok(KadResult::Complete) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Walk the given Iplds' links up to `max_depth` (or indefinitely for `None`). Will return
    /// any duplicate trees unless `unique` is `true`.
    ///
    /// More information and a `'static` lifetime version available at [`refs::iplds_refs`].
    pub fn refs<'a, Iter>(
        &'a self,
        iplds: Iter,
        max_depth: Option<u64>,
        unique: bool,
    ) -> impl Stream<Item = Result<refs::Edge, ipld::BlockError>> + Send + 'a
    where
        Iter: IntoIterator<Item = (Cid, Ipld)> + Send + 'a,
    {
        refs::iplds_refs(self, iplds, max_depth, unique)
    }

    /// Obtain the list of addresses of bootstrapper nodes that are currently used.
    pub async fn get_bootstrappers(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetBootstrappers(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Extend the list of used bootstrapper nodes with an additional address.
    /// Return value cannot be used to determine if the `addr` was a new bootstrapper, subject to
    /// change.
    pub async fn add_bootstrapper(&self, addr: MultiaddrWithPeerId) -> Result<Multiaddr, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddBootstrapper(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Remove an address from the currently used list of bootstrapper nodes.
    /// Return value cannot be used to determine if the `addr` was an actual bootstrapper, subject to
    /// change.
    pub async fn remove_bootstrapper(&self, addr: MultiaddrWithPeerId) -> Result<Multiaddr, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RemoveBootstrapper(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Clear the currently used list of bootstrapper nodes, returning the removed addresses.
    pub async fn clear_bootstrappers(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::ClearBootstrappers(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Restore the originally configured bootstrapper node list by adding them to the list of the
    /// currently used bootstrapper node address list; returns the restored addresses.
    pub async fn restore_bootstrappers(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RestoreBootstrappers(tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Exit daemon.
    pub async fn exit_daemon(mut self) {
        // FIXME: this is a stopgap measure needed while repo is part of the struct Ipfs instead of
        // the background task or stream. After that this could be handled by dropping.
        self.repo.shutdown();

        // ignoring the error because it'd mean that the background task had already been dropped
        let _ = self.to_task.try_send(IpfsEvent::Exit);
    }
}

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
struct IpfsFuture<Types: IpfsTypes> {
    swarm: TSwarm<Types>,
    repo_events: Fuse<Receiver<RepoEvent>>,
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
                        trace!("guessing the first match for {} to be {}", first, addr);
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

    fn start_add_listener_address(&mut self, addr: Multiaddr, ret: Option<Channel<Multiaddr>>) {
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
            if let Some(sender) = ret {
                let _ = sender.send(Err(format_err!("Cannot start listening to an unspecified address when there are pending specified addresses")));
            }
            return;
        }

        match self.listening_addresses.entry(addr) {
            Entry::Occupied(oe) => {
                if let Some(sender) = ret {
                    let _ = sender.send(Err(format_err!("Already adding a possibly ephemeral Multiaddr; wait for the first one to resolve before adding others: {}", oe.key())));
                }
            }
            Entry::Vacant(ve) => match Swarm::listen_on(&mut self.swarm, ve.key().to_owned()) {
                Ok(id) => {
                    ve.insert((id, ret));
                }
                Err(e) => {
                    if let Some(sender) = ret {
                        let _ = sender.send(Err(Error::from(e)));
                    }
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
                    IpfsEvent::Connect(target, ret) => {
                        ret.send(self.swarm.connect(target)).ok();
                    }
                    IpfsEvent::Addresses(ret) => {
                        let addrs = self.swarm.addrs();
                        ret.send(Ok(addrs)).ok();
                    }
                    IpfsEvent::Listeners(ret) => {
                        let listeners = Swarm::listeners(&self.swarm)
                            .cloned()
                            .collect::<Vec<Multiaddr>>();
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
                        addresses.extend(
                            Swarm::external_addresses(&self.swarm).map(|ar| ar.addr.clone()),
                        );
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
                        self.start_add_listener_address(addr, Some(ret));
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
                    IpfsEvent::GetClosestPeers(peer_id, ret) => {
                        let future = self.swarm.get_closest_peers(peer_id);
                        let _ = ret.send(future);
                    }
                    IpfsEvent::GetBitswapPeers(ret) => {
                        let peers = self
                            .swarm
                            .bitswap()
                            .connected_peers
                            .keys()
                            .cloned()
                            .collect();
                        let _ = ret.send(peers);
                    }
                    IpfsEvent::FindPeer(peer_id, local_only, ret) => {
                        let swarm_addrs = self.swarm.swarm.connections_to(&peer_id);
                        let locally_known_addrs = if !swarm_addrs.is_empty() {
                            swarm_addrs
                        } else {
                            self.swarm.kademlia().addresses_of_peer(&peer_id)
                        };
                        let addrs = if !locally_known_addrs.is_empty() || local_only {
                            Either::Left(locally_known_addrs)
                        } else {
                            Either::Right(self.swarm.get_closest_peers(peer_id))
                        };
                        let _ = ret.send(addrs);
                    }
                    IpfsEvent::GetProviders(cid, ret) => {
                        let future = self.swarm.get_providers(cid);
                        let _ = ret.send(future);
                    }
                    IpfsEvent::Provide(cid, ret) => {
                        let _ = ret.send(self.swarm.start_providing(cid));
                    }
                    IpfsEvent::DhtGet(key, quorum, ret) => {
                        let future = self.swarm.dht_get(key, quorum);
                        let _ = ret.send(future);
                    }
                    IpfsEvent::DhtPut(key, value, quorum, ret) => {
                        let future = self.swarm.dht_put(key, value, quorum);
                        let _ = ret.send(future);
                    }
                    IpfsEvent::GetBootstrappers(ret) => {
                        let list = self.swarm.get_bootstrappers();
                        let _ = ret.send(list);
                    }
                    IpfsEvent::AddBootstrapper(addr, ret) => {
                        let result = self.swarm.add_bootstrapper(addr);
                        let _ = ret.send(result);
                    }
                    IpfsEvent::RemoveBootstrapper(addr, ret) => {
                        let result = self.swarm.remove_bootstrapper(addr);
                        let _ = ret.send(result);
                    }
                    IpfsEvent::ClearBootstrappers(ret) => {
                        let list = self.swarm.clear_bootstrappers();
                        let _ = ret.send(list);
                    }
                    IpfsEvent::RestoreBootstrappers(ret) => {
                        let list = self.swarm.restore_bootstrappers();
                        let _ = ret.send(list);
                    }
                    IpfsEvent::Exit => {
                        // FIXME: we could do a proper teardown
                        return Poll::Ready(());
                    }
                }
            }

            // Poll::Ready(None) and Poll::Pending can be used to break out of the loop, clippy
            // wants this to be written with a `while let`.
            while let Poll::Ready(Some(evt)) = Pin::new(&mut self.repo_events).poll_next(ctx) {
                match evt {
                    RepoEvent::WantBlock(cid) => self.swarm.want_block(cid),
                    RepoEvent::UnwantBlock(cid) => self.swarm.bitswap().cancel_block(&cid),
                    RepoEvent::NewBlock(cid, ret) => {
                        // TODO: consider if cancel is applicable in cases where we provide the
                        // associated Block ourselves
                        self.swarm.bitswap().cancel_block(&cid);
                        // currently disabled; see https://github.com/rs-ipfs/rust-ipfs/pull/281#discussion_r465583345
                        // for details regarding the concerns about enabling this functionality as-is
                        if false {
                            let _ = ret.send(self.swarm.start_providing(cid));
                        } else {
                            let _ = ret.send(Err(anyhow!("not actively providing blocks yet")));
                        }
                    }
                    RepoEvent::RemovedBlock(cid) => self.swarm.stop_providing_block(&cid),
                }
            }

            done = true;
        }
    }
}

/// Bitswap statistics
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitswapStats {
    /// The number of IPFS blocks sent to other peers
    pub blocks_sent: u64,
    /// The number of bytes sent in IPFS blocks to other peers
    pub data_sent: u64,
    /// The number of IPFS blocks received from other peers
    pub blocks_received: u64,
    /// The number of bytes received in IPFS blocks from other peers
    pub data_received: u64,
    /// Duplicate blocks received (the block had already been received previously)
    pub dup_blks_received: u64,
    /// The number of bytes in duplicate blocks received
    pub dup_data_received: u64,
    /// The current peers
    pub peers: Vec<PeerId>,
    /// The wantlist of the local node
    pub wantlist: Vec<(Cid, ipfs_bitswap::Priority)>,
}

impl
    From<(
        ipfs_bitswap::Stats,
        Vec<PeerId>,
        Vec<(Cid, ipfs_bitswap::Priority)>,
    )> for BitswapStats
{
    fn from(
        (stats, peers, wantlist): (
            ipfs_bitswap::Stats,
            Vec<PeerId>,
            Vec<(Cid, ipfs_bitswap::Priority)>,
        ),
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

/// Node module provides an easy to use interface used in `tests/`.
mod node {
    use super::*;
    use std::convert::TryFrom;

    /// Node encapsulates everything to setup a testing instance so that multi-node tests become
    /// easier.
    pub struct Node {
        /// The Ipfs facade.
        pub ipfs: Ipfs<TestTypes>,
        /// The peer identifier on the network.
        pub id: PeerId,
        /// The listened to and externally visible addresses. The addresses are suffixed with the
        /// P2p protocol containing the node's PeerID.
        pub addrs: Vec<Multiaddr>,
        /// Stores the single background task spawned for the node.
        pub bg_task: tokio::task::JoinHandle<()>,
    }

    impl Node {
        /// Initialises a new `Node` with an in-memory store backed configuration.
        ///
        /// This will use the testing defaults for the `IpfsOptions`. If `IpfsOptions` has been
        /// initialised manually, use `Node::with_options` instead.
        pub async fn new<T: AsRef<str>>(name: T) -> Self {
            let mut opts = IpfsOptions::inmemory_with_generated_keys();
            opts.span = Some(trace_span!("ipfs", node = name.as_ref()));
            Self::with_options(opts).await
        }

        /// Connects to a peer at the given address.
        pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
            let addr = MultiaddrWithPeerId::try_from(addr).unwrap();
            self.ipfs.connect(addr).await
        }

        /// Returns a new `Node` based on `IpfsOptions`.
        pub async fn with_options(opts: IpfsOptions) -> Self {
            let id = opts.keypair.public().into_peer_id();

            // for future: assume UninitializedIpfs handles instrumenting any futures with the
            // given span

            let (ipfs, fut): (Ipfs<TestTypes>, _) =
                UninitializedIpfs::new(opts).start().await.unwrap();
            let bg_task = tokio::task::spawn(fut);
            let addrs = ipfs.identity().await.unwrap().1;

            Node {
                ipfs,
                id,
                addrs,
                bg_task,
            }
        }

        /// Returns the subscriptions for a `Node`.
        pub fn get_subscriptions(
            &self,
        ) -> &std::sync::Mutex<subscription::Subscriptions<Block, String>> {
            &self.ipfs.repo.subscriptions.subscriptions
        }

        /// Bootstraps the local node to join the DHT: it looks up the node's own ID in the
        /// DHT and introduces it to the other nodes in it; at least one other node must be
        /// known in order for the process to succeed. Subsequently, additional queries are
        /// ran with random keys so that the buckets farther from the closest neighbor also
        /// get refreshed.
        pub async fn bootstrap(&self) -> Result<KadResult, Error> {
            let (tx, rx) = oneshot_channel();

            self.to_task.clone().send(IpfsEvent::Bootstrap(tx)).await?;

            rx.await??.await.map_err(|e| anyhow!(e))
        }

        /// Add a known listen address of a peer participating in the DHT to the routing table.
        /// This is mandatory in order for the peer to be discoverable by other members of the
        /// DHT.
        pub async fn add_peer(&self, peer_id: PeerId, mut addr: Multiaddr) -> Result<(), Error> {
            // Kademlia::add_address requires the address to not contain the PeerId
            if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
                addr.pop();
            }

            self.to_task
                .clone()
                .send(IpfsEvent::AddPeer(peer_id, addr))
                .await?;

            Ok(())
        }

        /// Returns the Bitswap peers for the a `Node`.
        pub async fn get_bitswap_peers(&self) -> Result<Vec<PeerId>, Error> {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetBitswapPeers(tx))
                .await?;

            rx.await.map_err(|e| anyhow!(e))
        }

        /// Shuts down the `Node`.
        pub async fn shutdown(self) {
            self.ipfs.exit_daemon().await;
            let _ = self.bg_task.await;
        }
    }

    impl Deref for Node {
        type Target = Ipfs<TestTypes>;

        fn deref(&self) -> &Self::Target {
            &self.ipfs
        }
    }

    impl DerefMut for Node {
        fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
            &mut self.ipfs
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::make_ipld;
    use multihash::Sha2_256;

    #[tokio::test]
    async fn test_put_and_get_block() {
        let ipfs = Node::new("test_node").await;

        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);
    }

    #[tokio::test]
    async fn test_put_and_get_dag() {
        let ipfs = Node::new("test_node").await;

        let data = make_ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);
    }

    #[tokio::test]
    async fn test_pin_and_unpin() {
        let ipfs = Node::new("test_node").await;

        let data = make_ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();

        ipfs.insert_pin(&cid, false).await.unwrap();
        assert!(ipfs.is_pinned(&cid).await.unwrap());
        ipfs.remove_pin(&cid, false).await.unwrap();
        assert!(!ipfs.is_pinned(&cid).await.unwrap());
    }
}
