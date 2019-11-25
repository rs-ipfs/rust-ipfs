use ipfs::{Block, Ipfs, IpfsOptions, TestTypes};
use std::convert::TryInto;
use futures::{FutureExt, TryFutureExt};

fn main() {
    unimplemented!();
    /*let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::runtime::current_thread::block_on_all(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn(fut.unit_error().boxed().compat());
        ipfs.init_repo().await.unwrap();
        ipfs.open_repo().await.unwrap();

        // Create a Block
        ipfs.put_block(Block::from("block-provide")).await.unwrap();

        // Retrive a Block
        let block = ipfs.get_block(Block::from("block-want\n").cid()).await.unwrap();
        let contents: String = block.into();
        println!("block contents: {:?}", contents);

        // Add a file
        ipfs.add("./examples/block.data".into()).await.unwrap();

        // Get a file
        let path = "/QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW".try_into().unwrap();
        let file = ipfs.get(path).await.unwrap();
        let contents: String = file.into();
        println!("file contents: {:?}", contents);
    }.unit_error().boxed().compat()).unwrap();*/
}
