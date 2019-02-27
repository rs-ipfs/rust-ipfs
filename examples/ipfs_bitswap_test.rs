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
    let options = IpfsOptions::new();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);

    tokio::run_async(async move {
        // Start daemon and initialize repo
        tokio::spawn_async(ipfs.start_daemon());
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        // Create a Block
        await!(ipfs.put_block(Block::from("block-provide"))).unwrap();

        // Retrive a Block
        let block = await!(ipfs.get_block(Block::from("block-want\n").cid())).unwrap();
        let string: String = block.into();
        println!("block: {:?}", string);
    });
}
