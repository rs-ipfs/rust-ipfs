//! IPFS node implementation
//#![deny(missing_docs)]
#![deny(warnings)]
#![feature(async_await, await_macro, futures_api)]
#![feature(drain_filter)]

#[macro_use] extern crate log;
use futures::future::FutureObj;
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
mod future;
pub mod ipld;
pub mod p2p;
pub mod repo;

pub use self::block::{Block, Cid};
use self::config::ConfigFile;
use self::future::BlockFuture;
use self::ipld::IpldDag;
pub use self::ipld::{Ipld, IpldError, IpldPath};
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::repo::RepoTypes;
use self::repo::{create_repo, RepoOptions, Repo, BlockStore};

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
pub struct Ipfs<Types: IpfsTypes> {
    repo: Repo<Types>,
    dag: IpldDag<Types>,
    swarm: Option<TSwarm<Types>>,
    events: Arc<Mutex<VecDeque<IpfsEvent>>>,
}

enum IpfsEvent {
    WantBlock(Cid),
    ProvideBlock(Cid),
    UnprovideBlock(Cid),
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
    pub fn init_repo(&mut self) -> FutureObj<'static, Result<(), std::io::Error>> {
        self.repo.init()
    }

    /// Open the ipfs repo.
    pub fn open_repo(&mut self) -> FutureObj<'static, Result<(), std::io::Error>> {
        self.repo.open()
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&mut self, block: Block) -> FutureObj<'static, Result<Cid, std::io::Error>> {
        let events = self.events.clone();
        let block_store = self.repo.block_store.clone();
        FutureObj::new(Box::new(async move {
            let cid = await!(block_store.put(block))?;
            events.lock().unwrap().push_back(IpfsEvent::ProvideBlock(cid.clone()));
            Ok(cid)
        }))
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&mut self, cid: Cid) -> FutureObj<'static, Result<Block, std::io::Error>> {
        let events = self.events.clone();
        let block_store = self.repo.block_store.clone();
        FutureObj::new(Box::new(async move {
            if !await!(block_store.contains(cid.clone()))? {
                events.lock().unwrap().push_back(IpfsEvent::WantBlock(cid.clone()));
            }
            await!(BlockFuture::new(block_store, cid))
        }))
    }

    /// Remove block from the ipfs repo.
    pub fn remove_block(&mut self, cid: Cid) -> FutureObj<'static, Result<(), std::io::Error>> {
        self.events.lock().unwrap().push_back(IpfsEvent::UnprovideBlock(cid.clone()));
        self.repo.block_store.remove(cid)
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub fn put_dag(&self, ipld: Ipld) -> FutureObj<'static, Result<Cid, IpldError>> {
        self.dag.put(ipld)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub fn get_dag(&self, path: IpldPath) -> FutureObj<'static, Result<Option<Ipld>, IpldError>> {
        self.dag.get(path)
    }

    /// Start daemon.
    pub fn start_daemon(&mut self) -> IpfsFuture<Types> {
        let events = self.events.clone();
        let swarm = self.swarm.take().unwrap();
        IpfsFuture {
            events,
            swarm: Box::new(swarm),
        }
    }
}

pub struct IpfsFuture<Types: SwarmTypes> {
    swarm: Box<TSwarm<Types>>,
    events: Arc<Mutex<VecDeque<IpfsEvent>>>,
}

impl<Types: SwarmTypes> Future for IpfsFuture<Types> {
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let _self = self.get_mut();
        loop {
            for event in _self.events.lock().unwrap().drain(..) {
                match event {
                    IpfsEvent::WantBlock(cid) => {
                        _self.swarm.want_block(cid);
                    }
                    IpfsEvent::ProvideBlock(cid) => {
                        _self.swarm.provide_block(cid);
                    }
                    IpfsEvent::UnprovideBlock(cid) => {
                        _self.swarm.stop_providing_block(&cid);
                    }
                }
            }
            let poll = _self.swarm.poll().expect("Error while polling swarm");
            match poll {
                Async::Ready(Some(_)) => {},
                Async::Ready(None) => {
                    return Poll::Ready(Ok(()));
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
    /*
    use super::*;

    #[test]
    fn test_put_and_get_block() {
        let options = IpfsOptions::test();
        let mut ipfs = Ipfs::new(options);
        let block = Block::from("hello block\n");
        let cid = ipfs.put_block(block.clone());
        let future = ipfs.get_block(cid).map(move |new_block| {
            assert_eq!(block, new_block);
        });
        run_ipfs(ipfs, future);
    }*/
}
