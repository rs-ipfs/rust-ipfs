#![feature(async_await, await_macro, futures_api)]
use futures::future::FutureObj;
use futures::prelude::*;
use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};

#[derive(Clone)]
struct Types;

impl RepoTypes for Types {
    type TBlockStore = ipfs::repo::fs::FsBlockStore;
    type TDataStore = ipfs::repo::mem::MemDataStore;
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
    let cid = Block::from("hello block\n").cid();

    tokio::run(FutureObj::new(Box::new(async move {
        tokio::spawn(ipfs.start_daemon().compat());

        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.put_block(block)).unwrap();
        let block = await!(ipfs.get_block(cid));
        println!("Received block with contents: {:?}",
                 String::from_utf8_lossy(&block.data()));
        Ok(())
    })).compat());
}
