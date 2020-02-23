use ipfs::{Block, UninitializedIpfs, IpfsOptions, TestTypes};
use std::convert::TryInto;
use async_std::task;

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();

    task::block_on(async move {
        // Start daemon and initialize repo
        let (mut ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

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
    });
}
