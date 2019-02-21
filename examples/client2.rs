#![feature(async_await, await_macro, futures_api)]
use futures::future::FutureObj;
use futures::prelude::*;
use ipfs::{Block, Ipfs, IpfsOptions, Types};

fn main() {
    let options = IpfsOptions::test();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);
    let block = Block::from("hello block\n");
    let cid = Block::from("hello block2\n").cid();

    tokio::run(FutureObj::new(Box::new(async move {
        tokio::spawn(ipfs.start_daemon().compat());

        await!(ipfs.put_block(block));
        let block = await!(ipfs.get_block(cid));
        println!("Received block with contents: {:?}",
                 String::from_utf8_lossy(&block.data()));
        Ok(())
    })).compat());
}
