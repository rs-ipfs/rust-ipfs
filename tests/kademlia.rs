use async_std::future::timeout;
use cid::{Cid, Codec};
use ipfs::{Block, Node};
use libp2p::PeerId;
use log::LevelFilter;
use multihash::Sha2_256;
use std::time::Duration;

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
        nodes.push(Node::new().await);
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
    let extra_peer = Node::new().await;
    assert!(extra_peer
        .add_peer(peers[0].0.clone(), peers[0].1[0].clone())
        .await
        .is_ok());

    // check that kad::bootstrap works
    assert!(extra_peer.bootstrap().await.is_ok());

    // check that kad::get_closest_peers works
    assert!(nodes[0].get_closest_peers().await.is_ok());

    // add a Block to the extra peer
    let data = Box::from(&b"hello block\n"[..]);
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    extra_peer.put_block(Block {
        cid: cid.clone(),
        data: data.clone(),
    })
    .await
    .unwrap();

    async_std::task::spawn(async { async_std::task::sleep(Duration::from_secs(1)).await }).await;

    // add another peer, bootstrap it and try to get that Block
    let extra_peer2 = Node::new(false).await;
    assert!(extra_peer2
        .add_peer(peers[0].0.clone(), peers[0].1[0].clone())
        .await
        .is_ok());

    assert!(timeout(Duration::from_secs(10), extra_peer2.get_block(&cid)).await.is_ok());
}
