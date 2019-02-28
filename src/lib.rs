//! IPFS node implementation
//#![deny(missing_docs)]
#![deny(warnings)]
#![feature(async_await, await_macro, futures_api)]
#![feature(drain_filter)]
#![feature(try_trait)]

#[macro_use] extern crate log;
use futures::prelude::*;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::task::{Poll, Waker};
use tokio::prelude::{Async, Stream as StreamOld};

pub mod bitswap;
pub mod block;
mod config;
pub mod error;
mod future;
pub mod ipld;
pub mod p2p;
pub mod repo;
pub mod unixfs;

pub use self::block::{Block, Cid};
use self::config::ConfigFile;
pub use self::error::Error;
use self::ipld::IpldDag;
pub use self::ipld::{Ipld, IpldPath};
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
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
    type TDataStore = repo::fs::RocksDataStore;
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
        let config = ConfigFile::default();
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
    repo_events: Option<Receiver<RepoEvent>>,
    dag: IpldDag<Types>,
    swarm: Option<TSwarm<Types>>,
    exit_events: Vec<Sender<IpfsEvent>>,
}

enum IpfsEvent {
    Exit,
}

impl<Types: IpfsTypes> Ipfs<Types> {
    /// Creates a new ipfs node.
    pub fn new(options: IpfsOptions<Types>) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let (repo, repo_events) = create_repo(repo_options);
        let swarm_options = SwarmOptions::<Types>::from(&options);
        let swarm = create_swarm(swarm_options, repo.clone());
        let dag = IpldDag::new(repo.clone());

        Ipfs {
            repo,
            dag,
            repo_events: Some(repo_events),
            swarm: Some(swarm),
            exit_events: Vec::default(),
        }
    }

    /// Initialize the ipfs repo.
    pub fn init_repo(&self) -> impl Future<Output=Result<(), Error>> {
        self.repo.init()
    }

    /// Open the ipfs repo.
    pub fn open_repo(&self) -> impl Future<Output=Result<(), Error>> {
        self.repo.open()
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&self, block: Block) -> impl Future<Output=Result<Cid, Error>> {
        self.repo.put_block(block)
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&self, cid: &Cid) -> impl Future<Output=Result<Block, Error>> {
        self.repo.get_block(cid)
    }

    /// Remove block from the ipfs repo.
    pub fn remove_block(&self, cid: &Cid) -> impl Future<Output=Result<(), Error>> {
        self.repo.remove_block(cid)
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub fn put_dag(&self, ipld: Ipld) -> impl Future<Output=Result<Cid, Error>> {
        self.dag.put(ipld, cid::Codec::DagCBOR)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub fn get_dag(&self, path: IpldPath) -> impl Future<Output=Result<Ipld, Error>> {
        self.dag.get(path)
    }

    /// Adds a file into the ipfs repo.
    pub fn add(&self, path: PathBuf) -> impl Future<Output=Result<Cid, Error>> {
        let dag = self.dag.clone();
        async move {
            let file = await!(File::new(path))?;
            let cid = await!(file.put_unixfs_v1(&dag))?;
            Ok(cid)
        }
    }

    /// Gets a file from the ipfs repo.
    pub fn get(&self, path: IpldPath) -> impl Future<Output=Result<File, Error>> {
        File::get_unixfs_v1(&self.dag, path)
    }

    /// Start daemon.
    pub fn start_daemon(&mut self) -> Option<IpfsFuture<Types>> {
        self.repo_events.take().map(|repo_events|{
            let (sender, receiver) = channel::<IpfsEvent>();
            self.exit_events.push(sender);

            IpfsFuture {
                repo_events,
                exit_events: receiver,
                swarm: Box::new(self.swarm.take().unwrap()),
            }
        })
    }

    /// Exit daemon.
    pub fn exit_daemon(&mut self) {
        for s in self.exit_events.drain(..) {
            let _ = s.send(IpfsEvent::Exit);
        }
    }
}

pub struct IpfsFuture<Types: SwarmTypes> {
    swarm: Box<TSwarm<Types>>,
    repo_events: Receiver<RepoEvent>,
    exit_events: Receiver<IpfsEvent>,
}

impl<Types: SwarmTypes> Future for IpfsFuture<Types> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let _self = self.get_mut();
        loop {
            if let Ok(IpfsEvent::Exit) = _self.exit_events.try_recv() {
                return Poll::Ready(());
            }

            loop {
                if let Ok(event) = _self.repo_events.try_recv() {
                    match event {
                        RepoEvent::WantBlock(cid) => {
                            _self.swarm.want_block(cid);
                        }
                        RepoEvent::ProvideBlock(cid) => {
                            _self.swarm.provide_block(cid);
                        }
                        RepoEvent::UnprovideBlock(cid) => {
                            _self.swarm.stop_providing_block(&cid);
                        }
                    }
                } else {
                    break
                }
            }

            let poll = _self.swarm.poll().expect("Error while polling swarm");
            match poll {
                Async::Ready(Some(_)) => {},
                Async::Ready(None) => {
                    return Poll::Ready(());
                },
                Async::NotReady => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get_block() {
        let options = IpfsOptions::<TestTypes>::default();
        let mut ipfs = Ipfs::new(options);
        let block = Block::from("hello block\n");

        tokio::run_async(async move {
            let fut = ipfs.start_daemon().unwrap();
            tokio::spawn_async(fut);

            let cid = await!(ipfs.put_block(block.clone())).unwrap();
            let new_block = await!(ipfs.get_block(&cid)).unwrap();
            assert_eq!(block, new_block);

            ipfs.exit_daemon();
        });
    }

    #[test]
    fn test_put_and_get_dag() {
        let options = IpfsOptions::<TestTypes>::default();
        let mut ipfs = Ipfs::new(options);

        tokio::run_async(async move {
            let fut = ipfs.start_daemon().unwrap();
            tokio::spawn_async(fut);

            let data: Ipld = vec![-1, -2, -3].into();
            let cid = await!(ipfs.put_dag(data.clone())).unwrap();
            let new_data = await!(ipfs.get_dag(cid.into())).unwrap();
            assert_eq!(data, new_data);

            ipfs.exit_daemon();
        });
    }
}
