use cid::{Cid, Codec};
use ipfs::{p2p::MultiaddrWithPeerId, Block, Node};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use multihash::Sha2_256;
use std::{convert::TryInto, time::Duration};
use tokio::time::timeout;

fn strip_peer_id(addr: Multiaddr) -> Multiaddr {
    let MultiaddrWithPeerId { multiaddr, .. } = addr.try_into().unwrap();
    multiaddr.into()
}

/// Check if `Ipfs::find_peer` works without DHT involvement.
#[tokio::test(max_threads = 1)]
async fn find_peer_local() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (b_id, mut b_addrs) = node_b.identity().await.unwrap();
    let b_id = b_id.into_peer_id();

    node_a.connect(b_addrs[0].clone()).await.unwrap();

    // while node_a is connected to node_b, they know each other's
    // addresses and can find them without using the DHT
    let found_addrs = node_a.find_peer(b_id).await.unwrap();

    // remove Protocol::P2p from b_addrs
    for addr in &mut b_addrs {
        assert!(matches!(addr.pop(), Some(Protocol::P2p(_))));
    }

    assert_eq!(found_addrs, b_addrs);
}

// starts the specified number of nodes connected in a chain.
async fn start_nodes_in_chain(count: usize) -> (Vec<Node>, Vec<(PeerId, Multiaddr)>) {
    // fire up count nodes and register their PeerIds and
    // Multiaddrs (without the PeerId) for add_peer purposes
    let mut nodes = Vec::with_capacity(count);
    let mut ids_and_addrs = Vec::with_capacity(count);
    for i in 0..count {
        let node = Node::new(i.to_string()).await;

        let (key, mut addrs) = node.identity().await.unwrap();
        let id = key.into_peer_id();
        let addr = strip_peer_id(addrs.pop().unwrap());

        nodes.push(node);
        ids_and_addrs.push((id, addr));
    }

    // register the nodes' addresses so they can bootstrap against
    // one another in a chain; node 0 is only aware of the existence
    // of node 1 and so on; bootstrap them eagerly/quickly, so that
    // they don't have a chance to form the full picture in the DHT
    for i in 0..count {
        let (next_id, next_addr) = if i < count - 1 {
            ids_and_addrs[i + 1].clone()
        } else {
            // the last node in the chain also needs to know some address
            // in order to bootstrap, so give it its neighbour's information
            // and then bootstrap it as well
            ids_and_addrs[count - 2].clone()
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // make sure that the nodes are not actively connected to each other
    // and that we are actually going to be testing the DHT here
    for node in &nodes {
        assert!([1usize, 2].contains(&node.peers().await.unwrap().len()));
    }

    (nodes, ids_and_addrs)
}

/// Check if `Ipfs::find_peer` works using DHT.
#[tokio::test(max_threads = 1)]
async fn dht_find_peer() {
    // works for numbers >=2, though 2 would essentially just
    // be the same as find_peer_local, so it should be higher
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs) = start_nodes_in_chain(CHAIN_LEN).await;

    // node 0 now tries to find the address of the very last node in the
    // chain; the chain should be long enough for it not to automatically
    // be connected to it after the bootstrap
    let found_addrs = nodes[0]
        .find_peer(ids_and_addrs[CHAIN_LEN - 1].0.clone())
        .await
        .unwrap();

    assert_eq!(found_addrs, vec![ids_and_addrs[CHAIN_LEN - 1].1.clone()]);
}

#[tokio::test(max_threads = 1)]
async fn dht_get_closest_peers() {
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs) = start_nodes_in_chain(CHAIN_LEN).await;

    assert_eq!(
        nodes[0]
            .get_closest_peers(ids_and_addrs[0].0.clone())
            .await
            .unwrap()
            .len(),
        CHAIN_LEN - 1
    );
}

#[ignore = "targets an actual bootstrapper, so random failures can happen"]
#[tokio::test(max_threads = 1)]
async fn dht_popular_content_discovery() {
    let (bootstrapper_id, bootstrapper_addr): (PeerId, Multiaddr) = (
        "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
            .parse()
            .unwrap(),
        "/ip4/104.131.131.82/tcp/4001".parse().unwrap(),
    );

    let peer = Node::new("a").await;

    // connect it to one of the well-known bootstrappers
    assert!(peer
        .add_peer(bootstrapper_id, bootstrapper_addr)
        .await
        .is_ok());

    // the Cid of the IPFS logo
    let cid: Cid = "bafkreicncneocapbypwwe3gl47bzvr3pkpxmmobzn7zr2iaz67df4kjeiq"
        .parse()
        .unwrap();

    assert!(timeout(Duration::from_secs(10), peer.get_block(&cid))
        .await
        .is_ok());
}

/// Check if Ipfs::{get_providers, provide} does its job.
#[tokio::test(max_threads = 1)]
async fn dht_providing() {
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs) = start_nodes_in_chain(CHAIN_LEN).await;

    // the last node puts a block in order to have something to provide
    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    nodes[CHAIN_LEN - 1]
        .put_block(Block {
            cid: cid.clone(),
            data,
        })
        .await
        .unwrap();

    // the last node then provides the Cid
    nodes[CHAIN_LEN - 1].provide(cid.clone()).await.unwrap();

    // and the first one should be able to find it
    assert_eq!(
        nodes[0].get_providers(cid).await.unwrap(),
        vec![ids_and_addrs[CHAIN_LEN - 1].0.clone()]
    );
}
