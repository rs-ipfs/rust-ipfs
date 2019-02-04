use futures::prelude::*;
use ipfs::{Block, Ipfs, run_ipfs};

fn main() {
    let mut ipfs = Ipfs::new();
    let cid = Block::from("hello block\n").cid();
    println!("Looking for block {}", cid.to_string());
    let future = ipfs.get_block(cid).map(|block| {
        println!("Received block with contents: '{:?}'",
                 String::from_utf8_lossy(&block.data()));
    });
    run_ipfs(ipfs, future);
}
