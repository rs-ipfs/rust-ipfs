use ipfs::Node;
use libp2p::PeerId;
use log::LevelFilter;

const PEER_COUNT: usize = 20;

#[async_std::test]
async fn kademlia() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter(Some("async_std"), LevelFilter::Error)
        .init();

    // start up PEER_COUNT bootstrapper nodes
    let mut nodes = Vec::with_capacity(PEER_COUNT);
    for _ in 0..PEER_COUNT {
        nodes.push(Node::new(false).await);
    }

    // register the bootstrappers' ids and addresses
    let mut peers = Vec::with_capacity(PEER_COUNT);
    for node in &nodes {
        let (id, addrs) = node.identity().await.unwrap();
        let id = PeerId::from_public_key(id);

        peers.push((id, addrs));
    }

    // connect all the bootstrappers to one another
    for (i, (node_id, _)) in peers.iter().enumerate() {
        for (peer_id, addrs) in peers.iter().filter(|(peer_id, _)| peer_id != node_id) {
            nodes[i]
                .add_peer(peer_id.clone(), addrs[0].clone())
                .await
                .unwrap();
        }
    }

    // introduce an extra peer and connect it to one of the bootstrappers
    let extra_peer = Node::new(false).await;
    assert!(extra_peer
        .add_peer(peers[0].0.clone(), peers[0].1[0].clone())
        .await
        .is_ok());

    // call kad::bootstrap
    assert!(extra_peer.bootstrap().await.is_ok());

    // call kad::get_closest_peers
    assert!(nodes[0].get_closest_peers().await.is_ok());
}
