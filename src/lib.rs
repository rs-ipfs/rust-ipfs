//! IPFS node implementation
//#![deny(missing_docs)]
#![deny(warnings)]
#![feature(associated_type_defaults)]
#![feature(drain_filter)]
#[macro_use] extern crate log;
use futures::prelude::*;
use futures::try_ready;
use std::path::PathBuf;

pub mod bitswap;
pub mod block;
mod config;
mod future;
pub mod p2p;
pub mod repo;

pub use self::block::{Block, Cid};
use self::config::ConfigFile;
use self::future::BlockFuture;
pub use self::p2p::SwarmTypes;
use self::p2p::{create_swarm, SwarmOptions, TSwarm};
pub use self::repo::RepoTypes;
use self::repo::{create_repo, RepoOptions, BlockStore, Repo};

const IPFS_LOG: &str = "info";
const IPFS_PATH: &str = "~/.rust-ipfs";
const XDG_APP_NAME: &str = "rust-ipfs";
const CONFIG_FILE: &str = "config.json";

/// Default IPFS types.
pub struct Types;
impl RepoTypes for Types {
    type TBlockStore = repo::mem::MemBlockStore;
    type TDataStore = repo::mem::MemDataStore;
    type TRepo = repo::IpfsRepo<Self::TBlockStore, Self::TDataStore>;
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
        let ipfs_path = std::env::var("IPFS_PATH").unwrap_or(IPFS_PATH.into()).into();
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
    repo: Types::TRepo,
    swarm: TSwarm<Types>,
}

impl<Types: IpfsTypes> Ipfs<Types> {
    /// Creates a new ipfs node.
    pub fn new(options: IpfsOptions) -> Self {
        let repo_options = RepoOptions::<Types>::from(&options);
        let repo = create_repo(repo_options);
        let swarm_options = SwarmOptions::<Types>::from(&options);
        let swarm = create_swarm(swarm_options, repo.clone());

        Ipfs {
            repo,
            swarm,
        }
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&mut self, block: Block) -> Cid {
        let cid = self.repo.blocks().put(block);
        self.swarm.provide_block(&cid);
        cid
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&mut self, cid: Cid) -> BlockFuture<Types::TBlockStore> {
        if !self.repo.blocks().contains(&cid) {
            self.swarm.want_block(cid.clone());
        }
        BlockFuture::new(self.repo.blocks().to_owned(), cid)
    }

    /// Remove block from the ipfs repo.
    pub fn remove_block(&mut self, cid: Cid) {
        self.repo.blocks().remove(&cid);
        self.swarm.stop_providing_block(&cid);
    }
}

impl<Types: IpfsTypes> Future for Ipfs<Types> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        loop {
            match self.swarm.poll().expect("Error while polling swarm") {
                Async::Ready(Some(_)) => {}
                Async::Ready(None) | Async::NotReady => break
            }
        }

        Ok(Async::NotReady)
    }
}

/// Run IPFS until the future completes.
pub fn run_ipfs<F: Future<Item=(), Error=()> + Send + 'static, Types: IpfsTypes + 'static>(
    mut ipfs: Ipfs<Types>,
    mut future: F,
) {
    tokio::run(futures::future::poll_fn(move || {
        match future.poll() {
            Ok(Async::NotReady) => {
                try_ready!(ipfs.poll());
                Ok(Async::NotReady)
            },
            Ok(Async::Ready(value)) => Ok(Async::Ready(value)),
            Err(err) => Err(err),
        }
    }));
}

#[cfg(test)]
mod tests {
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
    }
}
