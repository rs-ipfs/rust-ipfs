#![feature(async_await, await_macro, futures_api)]
use ipfs::{Block, Ipfs, IpfsOptions, TestTypes};
use std::convert::TryInto;

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::run_async(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);
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
