use async_std::task;
use ipfs::{Block, IpfsOptions, TestTypes, UninitializedIpfs};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;
use std::convert::TryInto;

fn main() {
    env_logger::init();
    let options = IpfsOptions::<TestTypes>::default();

    // Note: this test is now at rust-ipfs/tests/exchange_block.rs

    task::block_on(async move {
        // Start daemon and initialize repo
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        // Create a Block
        let data = b"block-provide".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        ipfs.put_block(Block::new(data, cid)).await.unwrap();

        // Retrive a Block
        let data = b"block-want\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = ipfs.get_block(&cid).await.unwrap();
        let contents = core::str::from_utf8(block.data()).unwrap();
        println!("block contents: {:?}", contents);

        // Add a file
        ipfs.add("./examples/block.data".into()).await.unwrap();

        // Get a file
        let path = "/QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW"
            .try_into()
            .unwrap();
        let file = ipfs.get(path).await.unwrap();
        let contents: String = file.into();
        println!("file contents: {:?}", contents);

        ipfs.exit_daemon().await;
    });
}
