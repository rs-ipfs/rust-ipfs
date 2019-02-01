use futures::prelude::*;
use ipfs::{Block, Ipfs};
use std::str;

fn main() {
    let mut ipfs = Ipfs::new();
    let cid = Block::from("hello block\n").cid();
    let future = ipfs.get_block(cid).and_then(move |block| {
        let data = &block.data()[..];
        let content = str::from_utf8(data).unwrap();
        println!("block contains: {}", content);
        Ok(())
    }).map_err(|err| {
        println!("error:; {}", err);
    });
    tokio::run(future);
}
