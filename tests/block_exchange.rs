use cid::{Cid, Codec};
use ipfs::Block;
use multihash::Sha2_256;
use std::time::Duration;
use tokio::time::{sleep, timeout};

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

// start fetching the cid before storing the block on the source, causing a need for a retry which
// we dont yet have.
#[tokio::test]
async fn two_node_get_put() {
    let nodes = spawn_nodes(2, Topology::Line).await;
    let block = create_block();

    let downloaded = nodes[1].get_block(&block.cid);
    tokio::pin!(downloaded);

    {
        let deadline = sleep(Duration::from_secs(1));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    // FIXME(flaky time assumption): we now assume that the want has been
                    // propagated, and that our nodes[0] has not found it locally. perhaps if swarm
                    // could propagate all of the events up, we could notify this via event. alas,
                    // we dont have such an event, nor can we hope to get this information over
                    // bitswap 1.0
                    break;
                },
                _ = &mut downloaded => unreachable!("cannot complete get_block before block exists"),
            }
        }
    }

    nodes[0].put_block(block.clone()).await.unwrap();

    let found_block = timeout(Duration::from_secs(10), downloaded)
        .await
        .expect("get_block did not complete in time")
        .unwrap();

    assert_eq!(block.data, found_block.data);
}

// check that a long line of nodes still works with get_block
#[tokio::test]
#[ignore]
async fn long_get_block() {
    tracing_subscriber::fmt::init();
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
