//! IPFS node implementation
#![deny(missing_docs)]
#![deny(warnings)]
#![feature(drain_filter)]
use libp2p::secio::SecioKeyPair;

mod bitswap;
pub mod block;
mod future;
mod p2p;
mod repo;

use self::bitswap::{strategy::AltruisticStrategy, Strategy};
pub use self::block::{Block, Cid};
use self::future::BlockFuture;
use self::p2p::{create_swarm, Swarm};
use self::repo::Repo;

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs {
    repo: Repo,
    strategy: AltruisticStrategy,
    swarm: Swarm,
}

impl Ipfs {
    /// Creates a new ipfs node.
    pub fn new() -> Self {
        let repo = Repo::new();
        let local_key = SecioKeyPair::ed25519_generated().unwrap();
        let strategy = AltruisticStrategy::new(repo.clone());
        let swarm = create_swarm(local_key);

        Ipfs {
            repo,
            strategy,
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    #[test]
    fn test_put_and_get_block() {
        let mut ipfs = Ipfs::new();
        let block = Block::from("hello block\n");
        let cid = ipfs.put_block(block.clone());
        let future = ipfs.get_block(cid).and_then(move |new_block| {
            assert_eq!(block, new_block);
            Ok(())
        }).map_err(|err| panic!(err));
        tokio::run(future);
    }
}
