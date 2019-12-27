//! IPFS node implementation
//#![deny(missing_docs)]

#[macro_use] extern crate failure;
#[macro_use] extern crate log;
pub use libp2p::PeerId;
use std::marker::PhantomData;
use std::path::PathBuf;
use futures::channel::mpsc::{channel, Sender, Receiver};
use std::future::Future;

pub mod bitswap;
pub mod block;
mod config;
pub mod error;
pub mod ipld;
pub mod ipns;
pub mod p2p;
pub mod path;
pub mod repo;
pub mod unixfs;

pub use self::block::{Block, Cid};
use self::config::ConfigFile;
pub use self::error::Error;
use self::ipld::IpldDag;
pub use self::ipld::Ipld;
use self::ipns::Ipns;
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::path::IpfsPath;
pub use self::repo::RepoTypes;
use self::repo::{create_repo, RepoOptions, Repo, RepoEvent};
use self::unixfs::File;

static IPFS_LOG: &str = "info";
static IPFS_PATH: &str = ".rust-ipfs";
static XDG_APP_NAME: &str = "rust-ipfs";
static CONFIG_FILE: &str = "config.json";

/// All types can be changed at compile time by implementing
/// `IpfsTypes`.
pub trait IpfsTypes: SwarmTypes + RepoTypes {}
impl<T: RepoTypes> SwarmTypes for T {
    type TStrategy = bitswap::strategy::AltruisticStrategy<Self>;
}
impl<T: SwarmTypes + RepoTypes> IpfsTypes for T {}

/// Default IPFS types.
#[derive(Clone)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    #[cfg(feature = "rocksdb")]
    type TDataStore = repo::fs::RocksDataStore;
    #[cfg(not(feature = "rocksdb"))]
    type TDataStore = repo::mem::MemDataStore;
}

/// Testing IPFS types
#[derive(Clone)]
pub struct TestTypes;
impl RepoTypes for TestTypes {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
}

/// Ipfs options
#[derive(Clone, Debug)]
pub struct IpfsOptions<Types: IpfsTypes> {
    _marker: PhantomData<Types>,
    /// The ipfs log level that should be passed to env_logger.
    pub ipfs_log: String,
    /// The path of the ipfs repo.
    pub ipfs_path: PathBuf,
    /// The ipfs config.
    pub config: ConfigFile,
}

impl Default for IpfsOptions<Types> {
    /// Create `IpfsOptions` from environment.
    fn default() -> Self {
        let ipfs_log = std::env::var("IPFS_LOG").unwrap_or(IPFS_LOG.into());
        let ipfs_path = std::env::var("IPFS_PATH").unwrap_or_else(|_| {
            let mut ipfs_path = std::env::var("HOME").unwrap_or("".into());
            ipfs_path.push_str("/");
            ipfs_path.push_str(IPFS_PATH);
            ipfs_path
        }).into();
        let xdg_dirs = xdg::BaseDirectories::with_prefix(XDG_APP_NAME).unwrap();
        let path = xdg_dirs.place_config_file(CONFIG_FILE).unwrap();
        let config = ConfigFile::new(path);

        IpfsOptions {
            _marker: PhantomData,
            ipfs_log,
            ipfs_path,
            config
        }
    }
}

impl Default for IpfsOptions<TestTypes> {
    /// Creates `IpfsOptions` for testing without reading or writing to the
    /// file system.
    fn default() -> Self {
        let ipfs_log = std::env::var("IPFS_LOG").unwrap_or(IPFS_LOG.into());
        let ipfs_path = std::env::var("IPFS_PATH").unwrap_or(IPFS_PATH.into()).into();
        let config = std::env::var("IPFS_TEST_CONFIG").map(|s| ConfigFile::new(s)).unwrap_or_else(|_| ConfigFile::default());
        IpfsOptions {
            _marker: PhantomData,
            ipfs_log,
            ipfs_path,
            config,
        }
    }
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs<Types: IpfsTypes> {
    repo: Repo<Types>,
    dag: IpldDag<Types>,
    ipns: Ipns<Types>,
    exit_events: Vec<Sender<IpfsEvent>>,
}

enum IpfsEvent {
    Exit,
}

/// Configured Ipfs instace or value which can be only initialized.
pub struct UninitializedIpfs<Types: IpfsTypes> {
    repo: Repo<Types>,
    dag: IpldDag<Types>,
    ipns: Ipns<Types>,
    moved_on_init: Option<(Receiver<RepoEvent>, TSwarm<Types>)>,
    exit_events: Vec<Sender<IpfsEvent>>,
}

impl<Types: IpfsTypes> UninitializedIpfs<Types> {
    /// Configures a new UninitializedIpfs with from the given options.
    pub fn new(options: IpfsOptions<Types>) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let (repo, repo_events) = create_repo(repo_options);
        let swarm_options = SwarmOptions::<Types>::from(&options);
        let swarm = create_swarm(swarm_options, repo.clone());
        let dag = IpldDag::new(repo.clone());
        let ipns = Ipns::new(repo.clone());

        UninitializedIpfs {
            repo,
            dag,
            ipns,
            moved_on_init: Some((repo_events, swarm)),
            exit_events: Vec::default(),
        }
    }

    /// Initialize the ipfs node.
    pub async fn start(mut self) -> Result<(Ipfs<Types>, impl std::future::Future<Output = ()>), Error> {
        use futures::compat::Stream01CompatExt;

        let (repo_events, swarm) = self.moved_on_init
            .take()
            .expect("Cant see how this should happen");

        self.repo.init().await?;
        self.repo.init().await?;

        let (sender, receiver) = channel::<IpfsEvent>(1);
        self.exit_events.push(sender);

        let fut = IpfsFuture {
            repo_events,
            exit_events: receiver,
            swarm: swarm.compat(),
        };

        let UninitializedIpfs { repo, dag, ipns, exit_events, .. } = self;

        Ok((Ipfs {
            repo,
            dag,
            ipns,
            exit_events
        }, fut))
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
    pub async fn put_dag(&self, ipld: Ipld) -> Result<IpfsPath, Error> {
        Ok(self.dag.put(ipld, cid::Codec::DagCBOR).await?)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        Ok(self.dag.get(path).await?)
    }

    /// Adds a file into the ipfs repo.
    pub async fn add(&self, path: PathBuf) -> Result<IpfsPath, Error> {
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

    /// Exit daemon.
    pub fn exit_daemon(mut self) {
        for mut s in self.exit_events.drain(..) {
            let _ = s.try_send(IpfsEvent::Exit);
        }
    }
}

pub struct IpfsFuture<Types: SwarmTypes> {
    swarm: futures::compat::Compat01As03<TSwarm<Types>>,
    repo_events: Receiver<RepoEvent>,
    exit_events: Receiver<IpfsEvent>,
}

use std::pin::Pin;
use std::task::{Poll, Context};

impl<Types: SwarmTypes> Future for IpfsFuture<Types> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        use futures::Stream;
        loop {
            // FIXME: this can probably be rewritten as a async { loop { select! { ... } } } once
            // libp2p uses std::future ... I couldn't figure out way to wrap it as compat,
            // box it and fuse it to for it to be used with futures::select!

            {
                let pin = Pin::new(&mut self.exit_events);

                if let Poll::Ready(Some(IpfsEvent::Exit)) = pin.poll_next(ctx) {
                    return Poll::Ready(());
                }
            }

            {

                loop {
                    let pin = Pin::new(&mut self.repo_events);
                    match pin.poll_next(ctx) {
                        Poll::Ready(Some(RepoEvent::WantBlock(cid))) =>
                            self.swarm.get_mut().want_block(cid),
                        Poll::Ready(Some(RepoEvent::ProvideBlock(cid))) =>
                            self.swarm.get_mut().provide_block(cid),
                        Poll::Ready(Some(RepoEvent::UnprovideBlock(cid))) =>
                            self.swarm.get_mut().stop_providing_block(&cid),
                        Poll::Ready(None) => panic!("other side closed the repo_events?"),
                        Poll::Pending => break,
                    }

                }
            }

            {
                let poll = Pin::new(&mut self.swarm).poll_next(ctx);
                match poll {
                    Poll::Ready(Some(_)) => {},
                    Poll::Ready(None) => { return Poll::Ready(()); },
                    Poll::Pending => { return Poll::Pending; }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::{FutureExt, TryFutureExt};

    /// Testing helper for std::future::Futures until we can upgrade tokio
    pub(crate) fn async_test<O, F>(future: F) -> O
        where O: 'static + Send,
              F: std::future::Future<Output = O> + 'static + Send
    {
        let (tx, rx) = std::sync::mpsc::channel();
        tokio::run(async move {
            let tx = tx;
            let awaited = future.await;
            tx.send(awaited).unwrap();
        }.unit_error().boxed().compat());
        rx.recv().unwrap()
    }

    #[test]
    fn test_put_and_get_block() {
        async_test(async move {
            let options = IpfsOptions::<TestTypes>::default();
            let block = Block::from("hello block\n");
            let ipfs = Ipfs::new(options);
            let (mut ipfs, fut) = ipfs.start().await.unwrap();
            tokio::spawn(fut.unit_error().boxed().compat());

            let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
            let new_block = ipfs.get_block(&cid).await.unwrap();
            assert_eq!(block, new_block);

            ipfs.exit_daemon();
        });
    }

    #[test]
    fn test_put_and_get_dag() {
        let options = IpfsOptions::<TestTypes>::default();

        async_test(async move {

            let (ipfs, fut) = Ipfs::new(options).start().await.unwrap();
            tokio::spawn(fut.unit_error().boxed().compat());

            let data: Ipld = vec![-1, -2, -3].into();
            let cid = ipfs.put_dag(data.clone()).await.unwrap();
            let new_data = ipfs.get_dag(cid.into()).await.unwrap();
            assert_eq!(data, new_data);

            ipfs.exit_daemon();
        });
    }
}
