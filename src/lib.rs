//! IPFS node implementation
#![deny(missing_docs)]
#![deny(warnings)]

mod bitswap;
pub mod block;
mod future;
mod p2p;
mod repo;

use self::bitswap::Bitswap;
pub use self::block::{Block, Cid};
use self::future::BlockFuture;
use self::repo::Repo;

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs {
    bitswap: Bitswap,
    repo: Repo,
}

impl Ipfs {
    /// Creates a new ipfs node.
    pub fn new() -> Self {
        Ipfs {
            bitswap: Bitswap::new(),
            repo: Repo::new(),
        }
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&mut self, block: Block) -> Cid {
        self.repo.put(block)
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&mut self, cid: Cid) -> BlockFuture {
        if !self.repo.contains(&cid) {
            self.bitswap.get_block(cid.clone());
        }
        BlockFuture::new(self.repo.clone(), self.bitswap.clone(), cid)
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
