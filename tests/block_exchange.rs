use cid::{Cid, Codec};
use ipfs::Block;
use multihash::Sha2_256;
use std::time::Duration;
use tokio::time::timeout;

mod common;
use common::{spawn_nodes, Topology};

fn create_block() -> Block {
    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    Block { cid, data }
}

// verify that a put block can be received via get_block and the data matches
#[tokio::test]
async fn two_node_put_get() {
    let nodes = spawn_nodes(2, Topology::Line).await;
    let block = create_block();

    nodes[0].put_block(block.clone()).await.unwrap();
    let found_block = timeout(Duration::from_secs(10), nodes[1].get_block(&block.cid))
        .await
        .expect("get_block did not complete in time")
        .unwrap();

    assert_eq!(block.data, found_block.data);
}

// check that a long line of nodes still works with get_block
#[tokio::test]
#[ignore]
async fn long_get_block() {
    // this number could be higher, but it starts hanging above ~24
    const N: usize = 10;
    let nodes = spawn_nodes(N, Topology::Line).await;
    let block = create_block();

    // the first node should get the block from the last one...
    nodes[N - 1].put_block(block.clone()).await.unwrap();
    nodes[0].get_block(&block.cid).await.unwrap();

    // ...and the last one from the first one
    nodes[0].put_block(block.clone()).await.unwrap();
    nodes[N - 1].get_block(&block.cid).await.unwrap();
}
