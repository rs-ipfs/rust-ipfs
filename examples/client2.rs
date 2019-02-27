#![feature(async_await, await_macro, futures_api)]
use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};

#[derive(Clone)]
struct Types;

impl RepoTypes for Types {
    type TBlockStore = ipfs::repo::mem::MemBlockStore;
    type TDataStore = ipfs::repo::mem::MemDataStore;
}

impl SwarmTypes for Types {
    type TStrategy = ipfs::bitswap::strategy::AltruisticStrategy<Self>;
}

impl IpfsTypes for Types {}

fn main() {
    let options = IpfsOptions::test();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);
    let block = Block::from("hello block\n");
    let cid = Block::from("hello block2\n").cid().to_owned();

    tokio::run_async(async move {
        tokio::spawn_async(ipfs.start_daemon());

        await!(ipfs.put_block(block)).unwrap();
        let block = await!(ipfs.get_block(&cid)).unwrap();
        println!("Received block with contents: {:?}",
                 String::from_utf8_lossy(&block.data()));
    });
}
