use std::time::Duration;
use tokio::time;

mod common;
use common::{spawn_connected_nodes, Topology};

// Ensure that the Bitswap object doesn't leak.
#[tokio::test(max_threads = 1)]
async fn check_bitswap_cleanups() {
    // create a few nodes and connect the first one to others
    let mut nodes = spawn_connected_nodes(3, Topology::Star).await;

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 2);

    // last node says goodbye; check the number of bitswap peers
    if let Some(node) = nodes.pop() {
        node.shutdown().await;
        time::delay_for(Duration::from_millis(200)).await;
    }

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 1);

    // another node says goodbye; check the number of bitswap peers
    if let Some(node) = nodes.pop() {
        node.shutdown().await;
        time::delay_for(Duration::from_millis(200)).await;
    }

    let bitswap_peers = nodes[0].get_bitswap_peers().await.unwrap();
    assert!(bitswap_peers.is_empty());
}
