use futures::prelude::*;
use ipfs::{Block, Ipfs, IpfsOptions};

fn main() {
    let options = IpfsOptions::test();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);
    let block = Block::from("hello block\n");
    ipfs.put_block(block);
    let cid = Block::from("hello block2\n").cid();
    let future = ipfs.get_block(cid).map(|block| {
        println!("Received block with contents: '{:?}'",
                 String::from_utf8_lossy(&block.data()));
    });
    tokio::run(ipfs.join(future).map(|_| ()));
}
