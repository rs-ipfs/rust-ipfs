use cid::{Cid, Codec};
use ipfs::Block;
use multihash::Sha2_256;
use std::time::Duration;
use tokio::time;

mod common;
use common::{spawn_nodes, Topology};

// Ensure that the Bitswap object doesn't leak.
#[tokio::test]
async fn check_bitswap_cleanups() {
    // create a few nodes and connect the first one to others
    let mut nodes = spawn_nodes(3, Topology::Star).await;

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 2);

    // last node says goodbye; check the number of bitswap peers
    if let Some(node) = nodes.pop() {
        node.shutdown().await;
        time::sleep(Duration::from_millis(200)).await;
    }

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 1);

    // another node says goodbye; check the number of bitswap peers
    if let Some(node) = nodes.pop() {
        node.shutdown().await;
        time::sleep(Duration::from_millis(200)).await;
    }

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert!(bitswap_peers.is_empty());
}

// this test is designed to trigger unfavorable conditions for the bitswap
// protocol by putting blocks in every second node and attempting to get
// them from the other nodes; intended to be used for debugging or stress
// testing the bitswap protocol (though it would be advised to uncomment
// the tracing_subscriber for stress-testing purposes)
#[ignore]
#[tokio::test]
async fn bitswap_stress_test() {
    fn filter(i: usize) -> bool {
        i % 2 == 0
    }

    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let nodes = spawn_nodes(5, Topology::Mesh).await;

    for (i, node) in nodes.iter().enumerate() {
        if filter(i) {
            node.put_block(Block {
                cid: cid.clone(),
                data: data.clone(),
            })
            .await
            .unwrap();
        }
    }

    for (i, node) in nodes.iter().enumerate() {
        if !filter(i) {
            node.get_block(&cid).await.unwrap();
        }
    }
}
