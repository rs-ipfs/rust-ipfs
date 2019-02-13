use futures::prelude::*;
use ipfs::{Block, Ipfs, config::Configuration};

fn main() {
    env_logger::Builder::new()
        .parse(&std::env::var("IPFS_LOG").unwrap_or_default())
        .init();
    let mut ipfs = Ipfs::from_config(Configuration::generate());
    let block = Block::from("hello block\n");
    ipfs.put_block(block);
    let cid = Block::from("hello block2\n").cid();
    let future = ipfs.get_block(cid).map(|block| {
        println!("Received block with contents: '{:?}'",
                 String::from_utf8_lossy(&block.data()));
    });
    tokio::run(ipfs.join(future).map(|_| ()));
}
