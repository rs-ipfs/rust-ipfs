use futures::prelude::*;
use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};

struct Types;

impl RepoTypes for Types {
    type TBlockStore = ipfs::repo::mem::MemBlockStore;
    type TDataStore = ipfs::repo::mem::MemDataStore;
    type TRepo = ipfs::repo::IpfsRepo<Self::TBlockStore, Self::TDataStore>;
}

impl SwarmTypes for Types {
    type TStrategy = ipfs::bitswap::strategy::AltruisticStrategy<Self>;
}

impl IpfsTypes for Types {}

fn main() {
    let options = IpfsOptions::new();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);
    let block = Block::from("hello block2\n");
    ipfs.put_block(block);
    let cid = Block::from("hello block\n").cid();
    let future = ipfs.get_block(cid).map(|block| {
        println!("Received block with contents: '{}'",
                 String::from_utf8_lossy(&block.data()));
    });
    tokio::run(ipfs.join(future).map(|_| ()));
}
