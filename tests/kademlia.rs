use cid::{Cid, Codec};
use ipfs::{p2p::MultiaddrWithPeerId, Block, Node};
use libp2p::{kad::Quorum, multiaddr::Protocol, Multiaddr};
use multihash::Sha2_256;
use tokio::time::timeout;

use std::{convert::TryInto, time::Duration};

mod common;
use common::{interop::ForeignNode, spawn_nodes, Topology};

fn strip_peer_id(addr: Multiaddr) -> Multiaddr {
    let MultiaddrWithPeerId { multiaddr, .. } = addr.try_into().unwrap();
    multiaddr.into()
}

/// Check if `Ipfs::find_peer` works without DHT involvement.
#[tokio::test]
async fn find_peer_local() {
    let nodes = spawn_nodes(2, Topology::None).await;
    nodes[0].connect(nodes[1].addrs[0].clone()).await.unwrap();

    // while nodes[0] is connected to nodes[1], they know each
    // other's addresses and can find them without using the DHT
    let mut found_addrs = nodes[0].find_peer(nodes[1].id).await.unwrap();

    for addr in &mut found_addrs {
        addr.push(Protocol::P2p(nodes[1].id.into()));
        assert!(nodes[1].addrs.contains(addr));
    }
}

// starts the specified number of rust IPFS nodes connected in a chain.
#[cfg(all(not(feature = "test_go_interop"), not(feature = "test_js_interop")))]
async fn spawn_bootstrapped_nodes(n: usize) -> (Vec<Node>, Option<ForeignNode>) {
    // fire up `n` nodes
    let nodes = spawn_nodes(n, Topology::None).await;

    // register the nodes' addresses so they can bootstrap against
    // one another in a chain; node 0 is only aware of the existence
    // of node 1 and so on; bootstrap them eagerly/quickly, so that
    // they don't have a chance to form the full picture in the DHT
    for i in 0..n {
        let (next_id, next_addr) = if i < n - 1 {
            (nodes[i + 1].id, nodes[i + 1].addrs[0].clone())
        } else {
            // the last node in the chain also needs to know some address
            // in order to bootstrap, so give it its neighbour's information
            // and then bootstrap it as well
            (nodes[n - 2].id, nodes[n - 2].addrs[0].clone())
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // make sure that the nodes are not actively connected to each other
    // and that we are actually going to be testing the DHT here
    for node in &nodes {
        assert!([1usize, 2].contains(&node.peers().await.unwrap().len()));
    }

    (nodes, None)
}

// most of the setup is the same as in the not(feature = "test_X_interop") case, with
// the addition of a foreign node in the middle of the chain; the first half of the chain
// learns about the next peer, the foreign node being the last one, and the second half
// learns about the previous peer, the foreign node being the first one; a visualization:
// r[0] > r[1] > .. > foreign < .. < r[n - 3] < r[n - 2]
#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
async fn spawn_bootstrapped_nodes(n: usize) -> (Vec<Node>, Option<ForeignNode>) {
    // start a foreign IPFS node
    let foreign_node = ForeignNode::new();

    // exclude one node to make room for the intermediary foreign node
    let nodes = spawn_nodes(n - 1, Topology::None).await;

    // skip the last index again, as there is a foreign node without one bound to it
    for i in 0..(n - 1) {
        let (next_id, next_addr) = if i == n / 2 - 1 || i == n / 2 {
            println!("telling rust node {} about the foreign node", i);
            (foreign_node.id, foreign_node.addrs[0].clone())
        } else if i < n / 2 {
            println!("telling rust node {} about rust node {}", i, i + 1);
            (nodes[i + 1].id, nodes[i + 1].addrs[0].clone())
        } else {
            println!("telling rust node {} about rust node {}", i, i - 1);
            (nodes[i - 1].id, nodes[i - 1].addrs[0].clone())
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // in this case we can't make sure that all the nodes only have 1 or 2 peers but it's not a big
    // deal, since in reality this kind of extreme conditions are unlikely and we already test that
    // in the pure-rust setup

    (nodes, Some(foreign_node))
}

/// Check if `Ipfs::find_peer` works using DHT.
#[tokio::test]
async fn dht_find_peer() {
    // works for numbers >=2, though 2 would essentially just
    // be the same as find_peer_local, so it should be higher
    const CHAIN_LEN: usize = 10;
    let (nodes, foreign_node) = spawn_bootstrapped_nodes(CHAIN_LEN).await;
    let last_index = CHAIN_LEN - if foreign_node.is_none() { 1 } else { 2 };

    // node 0 now tries to find the address of the very last node in the
    // chain; the chain should be long enough for it not to automatically
    // be connected to it after the bootstrap
    let found_addrs = nodes[0].find_peer(nodes[last_index].id).await.unwrap();

    let to_be_found = strip_peer_id(nodes[last_index].addrs[0].clone());
    assert_eq!(found_addrs, vec![to_be_found]);
}

#[tokio::test]
async fn dht_get_closest_peers() {
    const CHAIN_LEN: usize = 10;
    let (nodes, _foreign_node) = spawn_bootstrapped_nodes(CHAIN_LEN).await;

    assert_eq!(
        nodes[0].get_closest_peers(nodes[0].id).await.unwrap().len(),
        CHAIN_LEN - 1
    );
}

#[ignore = "targets an actual bootstrapper, so random failures can happen"]
#[tokio::test]
async fn dht_popular_content_discovery() {
    let peer = Node::new("a").await;

    peer.restore_bootstrappers().await.unwrap();

    // the Cid of the IPFS logo
    let cid: Cid = "bafkreicncneocapbypwwe3gl47bzvr3pkpxmmobzn7zr2iaz67df4kjeiq"
        .parse()
        .unwrap();

    assert!(timeout(Duration::from_secs(10), peer.get_block(&cid))
        .await
        .is_ok());
}

/// Check if Ipfs::{get_providers, provide} does its job.
#[tokio::test]
async fn dht_providing() {
    const CHAIN_LEN: usize = 10;
    let (nodes, foreign_node) = spawn_bootstrapped_nodes(CHAIN_LEN).await;
    let last_index = CHAIN_LEN - if foreign_node.is_none() { 1 } else { 2 };

    // the last node puts a block in order to have something to provide
    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    nodes[last_index]
        .put_block(Block {
            cid: cid.clone(),
            data,
        })
        .await
        .unwrap();

    // the last node then provides the Cid
    nodes[last_index].provide(cid.clone()).await.unwrap();

    // and the first node should be able to learn that the last one provides it
    assert!(nodes[0]
        .get_providers(cid)
        .await
        .unwrap()
        .contains(&nodes[last_index].id));
}

/// Check if Ipfs::{get, put} does its job.
#[tokio::test]
async fn dht_get_put() {
    const CHAIN_LEN: usize = 10;
    let (nodes, foreign_node) = spawn_bootstrapped_nodes(CHAIN_LEN).await;
    let last_index = CHAIN_LEN - if foreign_node.is_none() { 1 } else { 2 };

    let (key, value) = (b"key".to_vec(), b"value".to_vec());
    let quorum = Quorum::One;

    // the last node puts a key+value record
    nodes[last_index]
        .dht_put(key.clone(), value.clone(), quorum)
        .await
        .unwrap();

    // and the first node should be able to get it
    assert_eq!(nodes[0].dht_get(key, quorum).await.unwrap(), vec![value]);
}
