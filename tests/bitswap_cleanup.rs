use ipfs::Node;
use tokio::time;

async fn wait(millis: u64) {
    time::delay_for(std::time::Duration::from_millis(millis)).await;
}

// Ensure that the Bitswap object doesn't leak.
#[tokio::test(max_threads = 1)]
async fn check_bitswap_cleanups() {
    // create a few nodes
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;
    let node_c = Node::new("c").await;

    // connect node a to node b...
    let (_, mut b_addrs) = node_b.identity().await.unwrap();
    node_a.connect(b_addrs.pop().unwrap()).await.unwrap();
    let bitswap_peers = node_a.get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 1);

    // ...and to node c
    let (_, mut c_addrs) = node_c.identity().await.unwrap();
    node_a.connect(c_addrs.pop().unwrap()).await.unwrap();
    let bitswap_peers = node_a.get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 2);

    // node b says goodbye; check the number of bitswap peers
    node_b.shutdown().await;
    wait(200).await;
    let bitswap_peers = node_a.get_bitswap_peers().await.unwrap();
    assert_eq!(bitswap_peers.len(), 1);

    // node c says goodbye; check the number of bitswap peers
    node_c.shutdown().await;
    wait(200).await;
    let bitswap_peers = node_a.get_bitswap_peers().await.unwrap();
    assert!(bitswap_peers.is_empty());
}
