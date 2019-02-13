//! IPFS node implementation
#![deny(missing_docs)]
#![deny(warnings)]
#![feature(drain_filter)]
use futures::prelude::*;
use futures::try_ready;

mod bitswap;
pub mod block;
pub mod config;
mod future;
mod p2p;
mod repo;

use self::bitswap::{strategy::AltruisticStrategy, Strategy};
pub use self::block::{Block, Cid};
use self::config::{Configuration, NetworkConfig};
use self::future::BlockFuture;
use self::p2p::{create_swarm, Swarm};
use self::repo::Repo;

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs {
    repo: Repo,
    swarm: Swarm<AltruisticStrategy>,
}

impl Ipfs {
    /// Creates a new ipfs node.
    pub fn new() -> Self {
        Ipfs::from_config(Configuration::new())
    }

    /// Creates a new ipfs node from a configuration.
    pub fn from_config(config: Configuration) -> Self {
        let repo = Repo::new();
        let strategy = AltruisticStrategy::new(repo.clone());
        let mut swarm = create_swarm(NetworkConfig::from_config(&config, strategy));

        // Listen on all interfaces and whatever port the OS assigns
        let addr = libp2p::Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        println!("Listening on {:?}", addr);

        Ipfs {
            repo,
            swarm,
        }
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&mut self, block: Block) -> Cid {
        let cid = self.repo.put(block);
        self.swarm.provide_block(&cid);
        cid
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&mut self, cid: Cid) -> BlockFuture {
        if !self.repo.contains(&cid) {
            self.swarm.want_block(cid.clone());
        }
        BlockFuture::new(self.repo.clone(), cid)
    }

    /// Remove block from the ipfs repo.
    pub fn remove_block(&mut self, cid: Cid) {
        self.repo.remove(&cid);
        self.swarm.stop_providing_block(&cid);
    }
}

impl Future for Ipfs {
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
pub fn run_ipfs<F: Future<Item=(), Error=()> + Send + 'static>(
    mut ipfs: Ipfs,
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
        let mut ipfs = Ipfs::new();
        let block = Block::from("hello block\n");
        let cid = ipfs.put_block(block.clone());
        let future = ipfs.get_block(cid).map(move |new_block| {
            assert_eq!(block, new_block);
        });
        run_ipfs(ipfs, future);
    }
}
