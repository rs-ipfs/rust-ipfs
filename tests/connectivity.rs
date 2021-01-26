use ipfs::Node;
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::time::Duration;
use tokio::time::timeout;

#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
mod common;
#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
use common::interop::ForeignNode;

const TIMEOUT: Duration = Duration::from_secs(5);

// Make sure two instances of ipfs can be connected by `Multiaddr`.
#[tokio::test]
async fn connect_two_nodes_by_addr() {
    let node_a = Node::new("a").await;

    #[cfg(all(not(feature = "test_go_interop"), not(feature = "test_js_interop")))]
    let node_b = Node::new("b").await;
    #[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
    let node_b = ForeignNode::new();

    timeout(TIMEOUT, node_a.connect(node_b.addrs[0].clone()))
        .await
        .expect("timeout")
        .expect("should have connected");
}

// Make sure only a `Multiaddr` with `/p2p/` can be used to connect.
#[tokio::test]
#[should_panic(expected = "called `Result::unwrap()` on an `Err` value: MissingProtocolP2p")]
async fn dont_connect_without_p2p() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let mut b_addr = node_b.addrs[0].clone();
    // drop the /p2p/peer_id part
    b_addr.pop();

    timeout(TIMEOUT, node_a.connect(b_addr))
        .await
        .expect("timeout")
        .expect_err("should not have connected");
}

// Make sure two instances of ipfs can be connected by `PeerId`.
#[ignore = "connecting just by PeerId is not currently supported"]
#[tokio::test]
async fn connect_two_nodes_by_peer_id() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    node_a
        .add_peer(node_b.id, node_b.addrs[0].clone())
        .await
        .unwrap();
    let b_id_multiaddr: Multiaddr = format!("/p2p/{}", &node_b.id).parse().unwrap();

    timeout(TIMEOUT, node_a.connect(b_id_multiaddr))
        .await
        .expect("timeout")
        .expect("should have connected");
}

// Ensure that duplicate connection attempts don't cause hangs.
#[tokio::test]
async fn connect_duplicate_multiaddr() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    // test duplicate connections by address
    for _ in 0..3 {
        // final success or failure doesn't matter, there should be no timeout
        let _ = timeout(TIMEOUT, node_a.connect(node_b.addrs[0].clone()))
            .await
            .unwrap();
    }
}

// More complicated one to the above; first node will have two listening addresses and the second
// one should dial both of the addresses, resulting in two connections.
#[tokio::test]
async fn connect_two_nodes_with_two_connections_doesnt_panic() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    node_a
        .add_listening_address("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();

    let addresses = node_a.addrs_local().await.unwrap();
    assert_eq!(addresses.len(), 2);

    for mut addr in addresses.into_iter() {
        addr.push(Protocol::P2p(node_a.id.into()));

        timeout(TIMEOUT, node_b.connect(addr))
            .await
            .expect("timeout")
            .expect("should have connected");
    }

    // not too sure on this, since there'll be a single peer but two connections; the return
    // type is `Vec<Connection>` but it's peer with any connection.
    let mut peers = node_a.peers().await.unwrap();
    assert_eq!(peers.len(), 1);

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

#[tokio::test]
async fn connect_to_wrong_peer() {
    let a = Node::new("a").await;
    let b = Node::new("b").await;
    let c = Node::new("c").await;

    // take b's address but with c's peerid
    let mut wrong_addr = b.addrs[0].clone();
    assert!(matches!(wrong_addr.pop(), Some(Protocol::P2p(_))));
    wrong_addr.push(Protocol::P2p(c.id.as_ref().to_owned()));

    // timeout of one is not great, but it's enough to make the connection.
    let connection_result = timeout(Duration::from_secs(1), a.connect(wrong_addr)).await;

    for &(node, name) in &[(&c, "c"), (&b, "b"), (&a, "a")] {
        let peers = node.peers().await.unwrap();
        assert!(
            peers.is_empty(),
            "{} should have no connections, but had: {:?}",
            name,
            peers
        );
    }

    connection_result
        .expect("connect timed out")
        .expect_err("connection should had failed (wrong peer id)");
}
