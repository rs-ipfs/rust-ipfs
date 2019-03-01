//! IPFS node implementation
//#![deny(missing_docs)]
#![deny(warnings)]
#![feature(async_await, await_macro, futures_api)]
#![feature(drain_filter)]

#[macro_use] extern crate log;
use futures::prelude::*;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use tokio::prelude::{Async, Stream as StreamOld};

pub mod bitswap;
pub mod block;
mod config;
pub mod future;
pub mod ipld;
pub mod p2p;
pub mod repo;

#[cfg(feature="server")]
pub mod server;

pub use self::block::{Block, Cid};
use self::config::ConfigFile;
use self::ipld::IpldDag;
pub use self::ipld::{Ipld, IpldError, IpldPath};
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::repo::RepoTypes;
use self::repo::{create_repo, RepoOptions, Repo, RepoEvent};

static IPFS_LOG: &str = "info";
static IPFS_PATH: &str = ".rust-ipfs";
static XDG_APP_NAME: &str = "rust-ipfs";
static CONFIG_FILE: &str = "config.json";

/// Default IPFS types.
#[derive(Clone)]
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::fs::FsBlockStore;
    type TDataStore = repo::mem::MemDataStore;
}
impl SwarmTypes for Types {
    type TStrategy = bitswap::strategy::AltruisticStrategy<Self>;
}
impl IpfsTypes for Types {}

/// All types can be changed at compile time by implementing
/// `IpfsTypes`. `IpfsOptions` implements the trait for default
/// implementations.
pub trait IpfsTypes: SwarmTypes + RepoTypes {}

/// Ipfs options
#[derive(Clone, Debug)]
pub struct IpfsOptions {
    /// The ipfs log level that should be passed to env_logger.
    pub ipfs_log: String,
    /// The path of the ipfs repo.
    pub ipfs_path: PathBuf,
    /// The ipfs config.
    pub config: ConfigFile,
}

impl IpfsOptions {
    /// Create `IpfsOptions` from environment.
    pub fn new() -> Self {
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
            ipfs_log,
            ipfs_path,
            config
        }
    }

    /// Creates `IpfsOptions` for testing without reading or writing to the
    /// file system.
    pub fn test() -> Self {
        let ipfs_log = std::env::var("IPFS_LOG").unwrap_or(IPFS_LOG.into());
        let ipfs_path = std::env::var("IPFS_PATH").unwrap_or(IPFS_PATH.into()).into();
        let config = ConfigFile::default();
        IpfsOptions {
            ipfs_log,
            ipfs_path,
            config,
        }
    }
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs<Types: IpfsTypes + Send + Sync> {
    repo: Repo<Types>,
    dag: IpldDag<Types>,
    swarm: Option<TSwarm<Types>>,
    events: Arc<Mutex<VecDeque<IpfsEvent>>>,
}

unsafe impl<Types: IpfsTypes + Send> Send for Ipfs<Types> {}
unsafe impl<Types: IpfsTypes + Sync> Sync for Ipfs<Types> {}

enum IpfsEvent {
    Exit,
}

impl<Types: IpfsTypes> Ipfs<Types> {
    /// Creates a new ipfs node.
    pub fn new(options: IpfsOptions) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let repo = create_repo(repo_options);
        let swarm_options = SwarmOptions::<Types>::from(&options);
        let swarm = create_swarm(swarm_options, repo.clone());
        let dag = IpldDag::new(repo.clone());

        Ipfs {
            repo,
            dag,
            swarm: Some(swarm),
            events: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Initialize the ipfs repo.
    pub fn init_repo(&self) -> impl Future<Output=Result<(), std::io::Error>> {
        self.repo.init()
    }

    /// Open the ipfs repo.
    pub fn open_repo(&self) -> impl Future<Output=Result<(), std::io::Error>> {
        self.repo.open()
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&self, block: Block) -> impl Future<Output=Result<Cid, std::io::Error>> {
        self.repo.put_block(block)
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&self, cid: &Cid) -> impl Future<Output=Result<Block, std::io::Error>> {
        self.repo.get_block(cid)
    }

    /// Remove block from the ipfs repo.
    pub fn remove_block(&self, cid: &Cid) -> impl Future<Output=Result<(), std::io::Error>> {
        self.repo.remove_block(cid)
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub fn put_dag(&self, ipld: Ipld) -> impl Future<Output=Result<Cid, IpldError>> {
        self.dag.put(ipld)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub fn get_dag(&self, path: IpldPath) -> impl Future<Output=Result<Ipld, IpldError>> {
        self.dag.get(path)
    }

    /// Start daemon.
    pub fn start_daemon(&mut self) -> IpfsFuture<Types> {
        IpfsFuture {
            events: self.events.clone(),
            repo: self.repo.clone(),
            swarm: Box::new(self.swarm.take().unwrap()),
        }
    }

    /// Exit daemon.
    pub fn exit_daemon(&self) {
        self.events.lock().unwrap().push_back(IpfsEvent::Exit)
    }
}

pub struct IpfsFuture<Types: SwarmTypes> {
    swarm: Box<TSwarm<Types>>,
    repo: Repo<Types>,
    events: Arc<Mutex<VecDeque<IpfsEvent>>>,
}

impl<Types: SwarmTypes> Future for IpfsFuture<Types> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let _self = self.get_mut();
        loop {
            for event in _self.events.lock().unwrap().drain(..) {
                match event {
                    IpfsEvent::Exit => {
                        return Poll::Ready(());
                    }
                }
            }
            for event in _self.repo.events.lock().unwrap().drain(..) {
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

    #[derive(Clone)]
    struct Types;

    impl RepoTypes for Types {
        type TBlockStore = crate::repo::mem::MemBlockStore;
        type TDataStore = crate::repo::mem::MemDataStore;
    }

    impl SwarmTypes for Types {
        type TStrategy = crate::bitswap::strategy::AltruisticStrategy<Self>;
    }

    impl IpfsTypes for Types {}

    #[test]
    fn test_put_and_get_block() {
        let options = IpfsOptions::test();
        let mut ipfs = Ipfs::<Types>::new(options);
        let block = Block::from("hello block\n");

        tokio::run_async(async move {
            tokio::spawn_async(ipfs.start_daemon());

            let cid = await!(ipfs.put_block(block.clone())).unwrap();
            let new_block = await!(ipfs.get_block(&cid)).unwrap();
            assert_eq!(block, new_block);

            ipfs.exit_daemon();
        });
    }

    #[test]
    fn test_put_and_get_dag() {
        let options = IpfsOptions::test();
        let mut ipfs = Ipfs::<Types>::new(options);

        tokio::run_async(async move {
            tokio::spawn_async(ipfs.start_daemon());

            let data: Ipld = vec![-1, -2, -3].into();
            let cid = await!(ipfs.put_dag(data.clone())).unwrap();
            let new_data = await!(ipfs.get_dag(cid.into())).unwrap();
            assert_eq!(data, new_data);

            ipfs.exit_daemon();
        });
    }
}
