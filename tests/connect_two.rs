use ipfs::Node;

// Make sure two instances of ipfs can be connected.
#[async_std::test]
async fn connect_two_nodes() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (_, b_addrs) = node_b.identity().await.unwrap();
    assert!(!b_addrs.is_empty());

    let mut connected = None;

    for addr in b_addrs {
        println!("trying {}", addr);
        match node_a.connect(addr.clone()).await {
            Ok(_) => {
                connected = Some(addr);
                break;
            }
            Err(e) => {
                println!("Failed connecting to {}: {}", addr, e);
            }
        }
    }

    let connected = connected.expect("Failed to connect to anything");
    println!("connected to {}", connected);
}

// More complicated one to the above; first node will have two listening addresses and the second
// one should dial both of the addresses, resulting in two connections.
#[async_std::test]
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
        "there should had been two local addresses, found {:?}",
        addresses
    );

    for addr in addresses {
        node_b.connect(addr).await.unwrap();
    }

    // not too sure on this, since there'll be a single peer but two connections; the return
    // type is `Vec<Connection>` but it's peer with any connection.
    let mut peers = node_a.peers().await.unwrap();
    assert_eq!(
        peers.len(),
        1,
        "there should had been one peer, found {:?}",
        peers
    );

    // sadly we are unable to currently verify that there exists two connections for the node_b
    // peer..

    node_a
        .disconnect(peers.remove(0).address)
        .await
        .expect("failed to disconnect peer_b at peer_a");

    let peers = node_a.peers().await.unwrap();
    assert!(
        peers.is_empty(),
        "node_b was still connected after disconnect: {:?}",
        peers
    );
}
