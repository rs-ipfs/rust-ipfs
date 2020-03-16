//! IPFS node implementation
//#![deny(missing_docs)]

#![cfg_attr(feature = "nightly", feature(external_doc))]
#![cfg_attr(feature = "nightly", doc(include = "../README.md"))]

#[macro_use]
extern crate log;
use async_std::path::PathBuf;
pub use bitswap::Block;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use futures::sink::SinkExt;
pub use libipld::cid::Cid;
use libipld::cid::Codec;
pub use libipld::ipld::Ipld;
pub use libp2p::{
    identity::{Keypair, PublicKey},
    Multiaddr, PeerId,
};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

mod config;
mod dag;
pub mod error;
pub mod ipns;
pub mod p2p;
pub mod path;
pub mod repo;
pub mod unixfs;

use self::config::ConfigFile;
use self::dag::IpldDag;
pub use self::error::Error;
use self::ipns::Ipns;
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::path::IpfsPath;
pub use self::repo::RepoTypes;
use self::repo::{create_repo, Repo, RepoEvent, RepoOptions};
use self::unixfs::File;

static IPFS_LOG: &str = "info";
static IPFS_PATH: &str = ".rust-ipfs";
static XDG_APP_NAME: &str = "rust-ipfs";
static CONFIG_FILE: &str = "config.json";

/// All types can be changed at compile time by implementing
/// `IpfsTypes`.
pub trait IpfsTypes: SwarmTypes + RepoTypes {}
impl<T: RepoTypes> SwarmTypes for T {
    type TStrategy = bitswap::AltruisticStrategy;
}
impl<T: SwarmTypes + RepoTypes> IpfsTypes for T {}

/// Default IPFS types.
#[derive(Clone, Debug)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    #[cfg(feature = "rocksdb")]
    type TDataStore = repo::fs::RocksDataStore;
    #[cfg(not(feature = "rocksdb"))]
    type TDataStore = repo::mem::MemDataStore;
}

/// Testing IPFS types
#[derive(Clone, Debug)]
pub struct TestTypes;
impl RepoTypes for TestTypes {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
}

/// Ipfs options
#[derive(Clone)]
pub struct IpfsOptions<Types: IpfsTypes> {
    _marker: PhantomData<Types>,
    /// The ipfs log level that should be passed to env_logger.
    pub ipfs_log: String,
    /// The path of the ipfs repo.
    pub ipfs_path: PathBuf,
    /// The keypair used with libp2p.
    pub keypair: libp2p::identity::Keypair,
    /// Nodes dialed during startup
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
}

impl<Types: IpfsTypes> fmt::Debug for IpfsOptions<Types> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("IpfsOptions")
            .field("ipfs_log", &self.ipfs_log)
            .field("ipfs_path", &self.ipfs_path)
            .field("bootstrap", &self.bootstrap)
            .field("keypair", &DebuggableKeypair(&self.keypair))
            .finish()
    }
}

use std::borrow::Borrow;

#[derive(Clone)]
struct DebuggableKeypair<I: Borrow<Keypair>>(I);

impl<I: Borrow<Keypair>> fmt::Debug for DebuggableKeypair<I> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.borrow() {
            Keypair::Ed25519(_) => "Ed25519",
            Keypair::Rsa(_) => "Rsa",
            Keypair::Secp256k1(_) => "Secp256k1",
        };

        write!(fmt, "Keypair::{}", kind)
    }
}

impl<I: Borrow<Keypair>> Borrow<Keypair> for DebuggableKeypair<I> {
    fn borrow(&self) -> &Keypair {
        self.0.borrow()
    }
}

impl<I: Borrow<Keypair>> DebuggableKeypair<I> {
    fn get(&self) -> &Keypair {
        self.borrow()
    }
}

impl<Types: IpfsTypes> IpfsOptions<Types> {
    pub fn new(ipfs_path: PathBuf, keypair: Keypair, bootstrap: Vec<(Multiaddr, PeerId)>) -> Self {
        Self {
            _marker: PhantomData,
            ipfs_log: String::from("trace"),
            ipfs_path,
            keypair,
            bootstrap,
        }
    }

    fn secio_key_pair(&self) -> &Keypair {
        &self.keypair
    }

    fn bootstrap(&self) -> &[(Multiaddr, PeerId)] {
        &self.bootstrap
    }
}

impl Default for IpfsOptions<Types> {
    /// Create `IpfsOptions` from environment.
    fn default() -> Self {
        let ipfs_log = std::env::var("IPFS_LOG").unwrap_or_else(|_| IPFS_LOG.into());
        let ipfs_path = std::env::var("IPFS_PATH")
            .unwrap_or_else(|_| {
                let mut ipfs_path = std::env::var("HOME").unwrap_or_else(|_| "".into());
                ipfs_path.push_str("/");
                ipfs_path.push_str(IPFS_PATH);
                ipfs_path
            })
            .into();
        let path = dirs::config_dir()
            .unwrap()
            .join(XDG_APP_NAME)
            .join(CONFIG_FILE);
        let config = ConfigFile::new(path);
        let keypair = config.secio_key_pair();
        let bootstrap = config.bootstrap();

        IpfsOptions {
            _marker: PhantomData,
            ipfs_log,
            ipfs_path,
            keypair,
            bootstrap,
        }
    }
}

impl Default for IpfsOptions<TestTypes> {
    /// Creates `IpfsOptions` for testing without reading or writing to the
    /// file system.
    fn default() -> Self {
        let ipfs_log = std::env::var("IPFS_LOG").unwrap_or_else(|_| IPFS_LOG.into());
        let ipfs_path = std::env::var("IPFS_PATH")
            .unwrap_or_else(|_| IPFS_PATH.into())
            .into();
        let config = std::env::var("IPFS_TEST_CONFIG")
            .map(ConfigFile::new)
            .unwrap_or_default();
        let keypair = config.secio_key_pair();
        let bootstrap = config.bootstrap();

        IpfsOptions {
            _marker: PhantomData,
            ipfs_log,
            ipfs_path,
            keypair,
            bootstrap,
        }
    }
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
#[derive(Clone, Debug)]
pub struct Ipfs<Types: IpfsTypes> {
    repo: Arc<Repo<Types>>,
    dag: IpldDag<Types>,
    ipns: Ipns<Types>,
    keys: DebuggableKeypair<Keypair>,
    to_task: Sender<IpfsEvent>,
}

/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
enum IpfsEvent {
    /// Request background task to return the listened and external addresses
    GetAddresses(OneshotSender<Vec<Multiaddr>>),
    Exit,
}

/// Configured Ipfs instace or value which can be only initialized.
pub struct UninitializedIpfs<Types: IpfsTypes> {
    repo: Arc<Repo<Types>>,
    dag: IpldDag<Types>,
    ipns: Ipns<Types>,
    keys: Keypair,
    moved_on_init: Option<(Receiver<RepoEvent>, TSwarm<Types>)>,
}

impl<Types: IpfsTypes> UninitializedIpfs<Types> {
    /// Configures a new UninitializedIpfs with from the given options.
    pub async fn new(options: IpfsOptions<Types>) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let keys = options.secio_key_pair().clone();
        let (repo, repo_events) = create_repo(repo_options);
        let swarm_options = SwarmOptions::<Types>::from(&options);
        let swarm = create_swarm(swarm_options, repo.clone()).await;
        let dag = IpldDag::new(repo.clone());
        let ipns = Ipns::new(repo.clone());

        UninitializedIpfs {
            repo,
            dag,
            ipns,
            keys,
            moved_on_init: Some((repo_events, swarm)),
        }
    }

    /// Initialize the ipfs node.
    pub async fn start(
        mut self,
    ) -> Result<(Ipfs<Types>, impl std::future::Future<Output = ()>), Error> {
        use futures::stream::StreamExt;

        let (repo_events, swarm) = self
            .moved_on_init
            .take()
            .expect("Cant see how this should happen");

        self.repo.init().await?;
        self.repo.init().await?;

        let (to_task, receiver) = channel::<IpfsEvent>(1);

        let fut = IpfsFuture {
            repo_events: repo_events.fuse(),
            from_facade: receiver.fuse(),
            swarm,
        };

        let UninitializedIpfs {
            repo,
            dag,
            ipns,
            keys,
            ..
        } = self;

        Ok((
            Ipfs {
                repo,
                dag,
                ipns,
                keys: DebuggableKeypair(keys),
                to_task,
            },
            fut,
        ))
    }
}

impl<Types: IpfsTypes> Ipfs<Types> {
    /// Puts a block into the ipfs repo.
    pub async fn put_block(&mut self, block: Block) -> Result<Cid, Error> {
        Ok(self.repo.put_block(block).await?)
    }

    /// Retrives a block from the ipfs repo.
    pub async fn get_block(&mut self, cid: &Cid) -> Result<Block, Error> {
        Ok(self.repo.get_block(cid).await?)
    }

    /// Remove block from the ipfs repo.
    pub async fn remove_block(&mut self, cid: &Cid) -> Result<(), Error> {
        Ok(self.repo.remove_block(cid).await?)
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        Ok(self.dag.put(ipld, Codec::DagCBOR).await?)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        Ok(self.dag.get(path).await?)
    }

    /// Adds a file into the ipfs repo.
    pub async fn add(&self, path: PathBuf) -> Result<Cid, Error> {
        let dag = self.dag.clone();
        let file = File::new(path).await?;
        let path = file.put_unixfs_v1(&dag).await?;
        Ok(path)
    }

    /// Gets a file from the ipfs repo.
    pub async fn get(&self, path: IpfsPath) -> Result<File, Error> {
        Ok(File::get_unixfs_v1(&self.dag, path).await?)
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        Ok(self.ipns.resolve(path).await?)
    }

    /// Publishes an ipld path.
    pub async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error> {
        Ok(self.ipns.publish(key, path).await?)
    }

    /// Cancel an ipns path.
    pub async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error> {
        self.ipns.cancel(key).await?;
        Ok(())
    }

    pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::GetAddresses(tx))
            .await?;
        let addresses = rx.await?;
        Ok((self.keys.get().public(), addresses))
    }

    /// Exit daemon.
    pub async fn exit_daemon(mut self) {
        // ignoring the error because it'd mean that the background task would had already been
        // dropped
        let _ = self.to_task.send(IpfsEvent::Exit).await;
    }
}

use futures::stream::Fuse;

// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
pub struct IpfsFuture<Types: SwarmTypes> {
    swarm: TSwarm<Types>,
    repo_events: Fuse<Receiver<RepoEvent>>,
    from_facade: Fuse<Receiver<IpfsEvent>>,
}

use std::pin::Pin;
use std::task::{Context, Poll};

impl<Types: SwarmTypes> Future for IpfsFuture<Types> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        use futures::Stream;
        use libp2p::Swarm;
        loop {
            // FIXME: this can probably be rewritten as a async { loop { select! { ... } } } once
            // libp2p uses std::future ... I couldn't figure out way to wrap it as compat,
            // box it and fuse it to for it to be used with futures::select!

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
                    IpfsEvent::GetAddresses(ret) => {
                        // perhaps this could be moved under `IpfsEvent` or free functions?
                        let mut addresses = Vec::new();
                        addresses.extend(Swarm::listeners(&self.swarm).cloned());
                        addresses.extend(Swarm::external_addresses(&self.swarm).cloned());
                        // ignore error, perhaps caller went away already
                        let _ = ret.send(addresses);
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
                    RepoEvent::ProvideBlock(cid) => self.swarm.provide_block(cid),
                    RepoEvent::UnprovideBlock(cid) => self.swarm.stop_providing_block(&cid),
                }
            }

            {
                let poll = Pin::new(&mut self.swarm).poll_next(ctx);
                match poll {
                    Poll::Ready(Some(_)) => {}
                    Poll::Ready(None) => {
                        // this should never happen with libp2p swarm
                        return Poll::Ready(());
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::ipld;
    use multihash::Sha2_256;

    #[async_std::test]
    async fn test_put_and_get_block() {
        let options = IpfsOptions::<TestTypes>::default();
        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);
        let ipfs = UninitializedIpfs::new(options).await;
        let (mut ipfs, fut) = ipfs.start().await.unwrap();
        task::spawn(fut);

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);

        ipfs.exit_daemon().await;
    }

    #[async_std::test]
    async fn test_put_and_get_dag() {
        let options = IpfsOptions::<TestTypes>::default();

        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);

        ipfs.exit_daemon().await;
    }
}
