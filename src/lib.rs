//! IPFS node implementation
//#![deny(missing_docs)]
#![cfg_attr(feature = "nightly", feature(external_doc))]
#![cfg_attr(feature = "nightly", doc(include = "../README.md"))]

#[macro_use]
extern crate tracing;

pub use crate::ipld::Ipld;
use anyhow::{anyhow, format_err};
pub use bitswap::{BitswapEvent, Block, Stats};
pub use cid::Cid;
use cid::Codec;
use either::Either;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use futures::sink::SinkExt;
use futures::stream::{Fuse, Stream};
pub use libp2p::core::{
    connection::ListenerId, multiaddr::Protocol, ConnectedPoint, Multiaddr, PeerId, PublicKey,
};
pub use libp2p::identity::Keypair;
use libp2p::swarm::NetworkBehaviour;
use std::path::PathBuf;
use tracing::Span;
use tracing_futures::Instrument;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use std::task::{Context, Poll};

mod config;
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

use self::dag::IpldDag;
pub use self::error::Error;
use self::ipns::Ipns;
pub use self::p2p::pubsub::{PubsubMessage, SubscriptionStream};
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::p2p::{Connection, KadResult, MultiaddrWithPeerId, MultiaddrWithoutPeerId};
pub use self::path::IpfsPath;
use self::repo::{create_repo, Repo, RepoEvent, RepoOptions};
pub use self::repo::{PinKind, PinMode, RepoTypes};
use self::subscription::SubscriptionFuture;

/// All types can be changed at compile time by implementing
/// `IpfsTypes`.
pub trait IpfsTypes: RepoTypes {}
impl<T: RepoTypes> IpfsTypes for T {}

/// Default IPFS types.
#[derive(Debug)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    type TDataStore = repo::fs::FsDataStore;
}

/// Testing IPFS types
#[derive(Debug)]
pub struct TestTypes;
impl RepoTypes for TestTypes {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
}

/// Ipfs options
#[derive(Clone)]
pub struct IpfsOptions {
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

impl fmt::Debug for IpfsOptions {
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

impl IpfsOptions {
    /// Creates an inmemory store backed node for tests
    pub fn inmemory_with_generated_keys() -> Self {
        Self {
            ipfs_path: std::env::temp_dir(),
            keypair: Keypair::generate_ed25519(),
            mdns: Default::default(),
            bootstrap: Default::default(),
            // default to lan kad for go-ipfs use in tests
            kad_protocol: Some("/ipfs/lan/kad/1.0.0".to_owned()),
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

impl IpfsOptions {
    pub fn new(
        ipfs_path: PathBuf,
        keypair: Keypair,
        bootstrap: Vec<(Multiaddr, PeerId)>,
        mdns: bool,
        kad_protocol: Option<String>,
    ) -> Self {
        Self {
            ipfs_path,
            keypair,
            bootstrap,
            mdns,
            kad_protocol,
        }
    }
}

impl Default for IpfsOptions {
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
            root.join(".rust-ipfs")
        };
        let config_path = dirs::config_dir()
            .unwrap()
            .join("rust-ipfs")
            .join("config.json");
        let config = ConfigFile::new(config_path).unwrap();
        let keypair = config.identity_key_pair();
        let bootstrap = config.bootstrap();

        IpfsOptions {
            ipfs_path,
            keypair,
            bootstrap,
            mdns: true,
            kad_protocol: None,
        }
    }
}

#[derive(Debug)]
pub struct Ipfs<Types: IpfsTypes>(Arc<IpfsInner<Types>>);

impl<Types: IpfsTypes> Clone for Ipfs<Types> {
    fn clone(&self) -> Self {
        Ipfs(Arc::clone(&self.0))
    }
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
#[derive(Debug)]
pub struct IpfsInner<Types: IpfsTypes> {
    pub span: Span,
    repo: Repo<Types>,
    keys: DebuggableKeypair<Keypair>,
    to_task: Sender<IpfsEvent>,
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
    WantList(Option<PeerId>, OneshotSender<Vec<(Cid, bitswap::Priority)>>),
    BitswapStats(OneshotSender<BitswapStats>),
    AddListeningAddress(Multiaddr, Channel<Multiaddr>),
    RemoveListeningAddress(Multiaddr, Channel<()>),
    Bootstrap(OneshotSender<Result<SubscriptionFuture<KadResult, String>, Error>>),
    AddPeer(PeerId, Multiaddr),
    GetClosestPeers(PeerId, OneshotSender<SubscriptionFuture<KadResult, String>>),
    GetBitswapPeers(OneshotSender<Vec<PeerId>>),
    FindPeer(
        PeerId,
        bool,
        OneshotSender<Either<Vec<Multiaddr>, SubscriptionFuture<KadResult, String>>>,
    ),
    GetProviders(Cid, OneshotSender<SubscriptionFuture<KadResult, String>>),
    Provide(
        Cid,
        OneshotSender<Result<SubscriptionFuture<KadResult, String>, Error>>,
    ),
    Exit,
}

/// Configured Ipfs instace or value which can be only initialized.
pub struct UninitializedIpfs<Types: IpfsTypes> {
    repo: Repo<Types>,
    span: Span,
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
    pub async fn new(options: IpfsOptions, span: Option<Span>) -> Self {
        let repo_options = RepoOptions::from(&options);
        let (repo, repo_events) = create_repo(repo_options);
        let keys = options.keypair.clone();
        let span = span.unwrap_or_else(|| trace_span!("ipfs"));

        UninitializedIpfs {
            repo,
            span,
            keys,
            options,
            repo_events,
        }
    }

    pub async fn default() -> Self {
        Self::new(IpfsOptions::default(), None).await
    }

    /// Initialize the ipfs node. The returned `Ipfs` value is cloneable, send and sync, and the
    /// future should be spawned on a executor as soon as possible.
    pub async fn start(self) -> Result<(Ipfs<Types>, impl Future<Output = ()>), Error> {
        use futures::stream::StreamExt;

        let UninitializedIpfs {
            repo,
            span,
            keys,
            repo_events,
            ..
        } = self;

        repo.init().await?;

        let (to_task, receiver) = channel::<IpfsEvent>(1);

        let ipfs = Ipfs(Arc::new(IpfsInner {
            span,
            repo,
            keys: DebuggableKeypair(keys),
            to_task,
        }));

        let swarm_options = SwarmOptions::from(&self.options);
        let swarm = create_swarm(swarm_options, ipfs.clone()).await?;

        let fut = IpfsFuture {
            repo_events: repo_events.fuse(),
            from_facade: receiver.fuse(),
            swarm,
            listening_addresses: HashMap::new(),
        };

        Ok((ipfs, fut))
    }
}

impl<Types: IpfsTypes> std::ops::Deref for Ipfs<Types> {
    type Target = IpfsInner<Types>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Types: IpfsTypes> Ipfs<Types> {
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

    /// Checks whether a given block is pinned. At the moment does not support incomplete recursive
    /// pins.
    ///
    /// Returns true if the block is pinned, false if not. See Crash unsafety notes for the false
    /// response.
    ///
    /// # Crash unsafety
    ///
    /// Cannot detect partially written recursive pins. Those can happen if `Ipfs::insert_pin(cid,
    /// recursive: true)` is interrupted by a crash for example.
    ///
    /// Works correctly only under no-crash situations. Workaround for hitting a crash is to re-pin
    /// any existing recursive pins.
    ///
    /// TODO: This operation could be provided as a `Ipfs::fix_pins()`.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let span = debug_span!(parent: &self.span, "is_pinned", cid = %cid);
        self.repo.is_pinned(cid).instrument(span).await
    }

    /// Lists all pins, or the specific kind thereof.
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
    /// `PinMode`.
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

    /// Puts an ipld dag node into the ipfs repo.
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        self.dag()
            .put(ipld, Codec::DagCBOR)
            .instrument(self.span.clone())
            .await
    }

    /// Gets an ipld dag node from the ipfs repo.
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

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        self.ipns()
            .resolve(path)
            .instrument(self.span.clone())
            .await
    }

    /// Publishes an ipld path.
    pub async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error> {
        self.ipns()
            .publish(key, path)
            .instrument(self.span.clone())
            .await
    }

    /// Cancel an ipns path.
    pub async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error> {
        self.ipns().cancel(key).instrument(self.span.clone()).await
    }

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

    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Addresses(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

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
    /// [`pubsub_unsubscribe`].
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

    pub async fn bitswap_wantlist(
        &self,
        peer: Option<PeerId>,
    ) -> Result<Vec<(Cid, bitswap::Priority)>, Error> {
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

    pub async fn refs_local(&self) -> Result<Vec<Cid>, Error> {
        self.repo.list_blocks().instrument(self.span.clone()).await
    }

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
                .send(IpfsEvent::FindPeer(peer_id.clone(), false, tx))
                .await?;

            match rx.await? {
                Either::Left(addrs) if !addrs.is_empty() => return Ok(addrs),
                Either::Left(_) => unreachable!(),
                Either::Right(future) => {
                    future.await?;

                    let (tx, rx) = oneshot_channel();

                    self.to_task
                        .clone()
                        .send(IpfsEvent::FindPeer(peer_id.clone(), true, tx))
                        .await?;

                    match rx.await? {
                        Either::Left(addrs) if !addrs.is_empty() => return Ok(addrs),
                        _ => return Err(anyhow!("couldn't find peer {}", peer_id)),
                    }
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Performs a DHT lookup for providers of a value to the given key.
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

    /// Exit daemon.
    pub async fn exit_daemon(self) {
        // FIXME: this is a stopgap measure needed while repo is part of the struct Ipfs instead of
        // the background task or stream. After that this could be handled by dropping.
        self.repo.shutdown();

        // ignoring the error because it'd mean that the background task had already been dropped
        let _ = self.to_task.clone().try_send(IpfsEvent::Exit);
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
                        let swarm_addrs = self.swarm.swarm.addresses_of_peer(&peer_id);
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
                    RepoEvent::ProvideBlock(cid, ret) => {
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
                    RepoEvent::UnprovideBlock(cid) => self.swarm.stop_providing_block(&cid),
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
    use std::convert::TryFrom;

    /// Node encapsulates everything to setup a testing instance so that multi-node tests become
    /// easier.
    pub struct Node {
        pub ipfs: Ipfs<TestTypes>,
        pub bg_task: tokio::task::JoinHandle<()>,
    }

    impl Node {
        pub async fn new<T: AsRef<str>>(name: T) -> Self {
            let opts = IpfsOptions::inmemory_with_generated_keys();
            Node::with_options(opts)
                .instrument(trace_span!("ipfs", node = name.as_ref()))
                .await
        }

        pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
            let addr = MultiaddrWithPeerId::try_from(addr).unwrap();
            self.ipfs.connect(addr).await
        }

        pub async fn with_options(opts: IpfsOptions) -> Self {
            let span = Some(Span::current());

            let (ipfs, fut): (Ipfs<TestTypes>, _) = UninitializedIpfs::new(opts, span)
                .in_current_span()
                .await
                .start()
                .in_current_span()
                .await
                .unwrap();

            let bg_task = tokio::task::spawn(fut.in_current_span());

            Node { ipfs, bg_task }
        }

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
        pub async fn add_peer(&self, peer_id: PeerId, addr: Multiaddr) -> Result<(), Error> {
            self.to_task
                .clone()
                .send(IpfsEvent::AddPeer(peer_id, addr))
                .await?;

            Ok(())
        }

        pub async fn get_bitswap_peers(&self) -> Result<Vec<PeerId>, Error> {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetBitswapPeers(tx))
                .await?;

            rx.await.map_err(|e| anyhow!(e))
        }

        pub async fn shutdown(self) {
            self.ipfs.exit_daemon().await;
            let _ = self.bg_task.await;
        }
    }

    impl std::ops::Deref for Node {
        type Target = Ipfs<TestTypes>;

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
    use crate::make_ipld;
    use libp2p::build_multiaddr;
    use multihash::Sha2_256;

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

    #[tokio::test(max_threads = 1)]
    async fn test_put_and_get_block() {
        let ipfs = Node::new("test_node").await;

        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);
    }

    #[tokio::test(max_threads = 1)]
    async fn test_put_and_get_dag() {
        let ipfs = Node::new("test_node").await;

        let data = make_ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);
    }

    #[tokio::test(max_threads = 1)]
    async fn test_pin_and_unpin() {
        let ipfs = Node::new("test_node").await;

        let data = make_ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();

        ipfs.insert_pin(&cid, false).await.unwrap();
        assert!(ipfs.is_pinned(&cid).await.unwrap());
        ipfs.remove_pin(&cid, false).await.unwrap();
        assert!(!ipfs.is_pinned(&cid).await.unwrap());
    }

    #[test]
    #[should_panic]
    fn default_ipfs_options_disabled_when_testing() {
        IpfsOptions::default();
    }
}
