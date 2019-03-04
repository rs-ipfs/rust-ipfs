#![feature(async_await, await_macro, futures_api)]
use ipfs::{Block, Ipfs, Cid, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use ipfs::{tokio_run, tokio_spawn};
use std::convert::TryInto;

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
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);
    let cid = Cid::from("QmR7tiySn6vFHcEjBeZNtYGAFh735PJHfEMdVEycj9jAPy").unwrap();

    tokio_run(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio_spawn(fut);
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        // Create a Block
        await!(ipfs.put_block(Block::from("block-provide"))).unwrap();

        // Retrive a Block
        let block = await!(ipfs.get_block(Block::from("block-want\n").cid())).unwrap();
        let contents: String = block.into();
        println!("block contents: {:?}", contents);

        // Add a file
        await!(ipfs.add("./examples/block.data".into())).unwrap();

        // Get a file
        let path = "/QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW".try_into().unwrap();
        let file = await!(ipfs.get(path)).unwrap();
        let contents: String = file.into();
        println!("file contents: {:?}", contents);
    });
}
