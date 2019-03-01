#![feature(async_await, await_macro, futures_api)]
use ipfs::{Block, Ipfs, Cid, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use ipfs::{tokio_run, tokio_spawn};

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
    let cid = Cid::from("QmR7tiySn6vFHcEjBeZNtYGAFh735PJHfEMdVEycj9jAPy").unwrap();

    tokio_run(async move {
        // Start daemon and initialize repo
        tokio_spawn(ipfs.start_daemon());
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        // Create a Block
        await!(ipfs.put_block(Block::from("block-provide"))).unwrap();

        // Retrive a Block
        let block = await!(ipfs.get_block(&cid)).unwrap();

        await!(ipfs.put_block(block)).unwrap();
        println!("block saved: {:}", cid);
    });
}
