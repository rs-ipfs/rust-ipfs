use ipfs::Node;
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::time::Duration;
use tokio::time::timeout;

// Make sure two instances of ipfs can be connected by `Multiaddr`.
#[tokio::test(max_threads = 1)]
async fn connect_two_nodes_by_addr() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (_, mut b_addrs) = node_b.identity().await.unwrap();
    let b_addr = b_addrs.pop().unwrap();

    timeout(Duration::from_secs(10), node_a.connect(b_addr))
        .await
        .expect("timeout")
        .expect("should've connected");
}

// Make sure only a `Multiaddr` with `/p2p/` can be used to connect.
#[tokio::test(max_threads = 1)]
#[should_panic(
    expected = "called `Result::unwrap()` on an `Err` value: Missing Protocol::P2p in the Multiaddr"
)]
async fn dont_connect_without_p2p() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (_, mut b_addrs) = node_b.identity().await.unwrap();
    let mut b_addr = b_addrs.pop().unwrap();
    // drop the /p2p/peer_id part
    b_addr.pop();

    timeout(Duration::from_secs(10), node_a.connect(b_addr))
        .await
        .expect("timeout")
        .expect_err("should not have connected");
}

// Make sure two instances of ipfs can be connected by `PeerId`.
// Currently ignored, as Ipfs::add_peer (that is necessary in
// order to connect by PeerId) already performs a dial to the
// given peer within Pubsub::add_node_to_partial_view it calls
#[ignore]
#[tokio::test(max_threads = 1)]
async fn connect_two_nodes_by_peer_id() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (b_key, mut b_addrs) = node_b.identity().await.unwrap();
    let b_id = b_key.into_peer_id();
    let b_id_multiaddr: Multiaddr = format!("/p2p/{}", b_id).parse().unwrap();

    while let Some(addr) = b_addrs.pop() {
        node_a.add_peer(b_id.clone(), addr).await.unwrap();
    }
    timeout(Duration::from_secs(10), node_a.connect(b_id_multiaddr))
        .await
        .expect("timeout")
        .expect("should've connected");
}

// Ensure that duplicate connection attempts don't cause hangs.
#[tokio::test(max_threads = 1)]
async fn connect_duplicate_multiaddr() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (_, mut b_addrs) = node_b.identity().await.unwrap();
    let b_addr = b_addrs.pop().unwrap();

    // test duplicate connections by address
    for _ in 0..3 {
        // final success or failure doesn't matter, there should be no timeout
        let _ = timeout(Duration::from_secs(1), node_a.connect(b_addr.clone()))
            .await
            .unwrap();
    }
}

// More complicated one to the above; first node will have two listening addresses and the second
// one should dial both of the addresses, resulting in two connections.
#[tokio::test(max_threads = 1)]
async fn connect_two_nodes_with_two_connections_doesnt_panic() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    node_a
        .add_listening_address(libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16)))
        .await
        .unwrap();

    let addresses = node_a.addrs_local().await.unwrap();
    assert_eq!(
        addresses.len(),
        2,
        "there should have been two local addresses, found {:?}",
        addresses
    );
    let node_a_id = node_a.identity().await.unwrap().0.into_peer_id();

    for mut addr in addresses.into_iter() {
        addr.push(Protocol::P2p(node_a_id.clone().into()));

        timeout(Duration::from_secs(10), node_b.connect(addr))
            .await
            .expect("timeout")
            .expect("should've connected");
    }

    // not too sure on this, since there'll be a single peer but two connections; the return
    // type is `Vec<Connection>` but it's peer with any connection.
    let mut peers = node_a.peers().await.unwrap();
    assert_eq!(
        peers.len(),
        1,
        "there should have been one peer, found {:?}",
        peers
    );

    // sadly we are unable to currently verify that there exists two connections for the node_b
    // peer..

    node_a
        .disconnect(peers.remove(0).addr)
        .await
        .expect("failed to disconnect peer_b at peer_a");

    let peers = node_a.peers().await.unwrap();
    assert!(
        peers.is_empty(),
        "node_b was still connected after disconnect: {:?}",
        peers
    );
}
