use cid::Cid;
use ipfs::{p2p::MultiaddrWithPeerId, IpfsOptions, Node};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
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

/// Check if `Ipfs::find_peer` works using DHT.
#[tokio::test(max_threads = 1)]
async fn find_peer_dht() {
    // the length of the chain the nodes will be connected in;
    // works for numbers >=2, though 2 would essentially just
    // be the same as find_peer_local, so it should be higher
    const CHAIN_LEN: usize = 10;

    // fire up CHAIN_LEN nodes and register their PeerIds and
    // Multiaddrs (without the PeerId) for add_peer purposes
    let mut nodes = Vec::with_capacity(CHAIN_LEN);
    let mut ids_and_addrs = Vec::with_capacity(CHAIN_LEN);
    for i in 0..CHAIN_LEN {
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
    for i in 0..CHAIN_LEN {
        let (next_id, next_addr) = if i < CHAIN_LEN - 1 {
            ids_and_addrs[i + 1].clone()
        } else {
            // the last node in the chain also needs to know some address
            // in order to bootstrap, so give it its neighbour's information
            // and then bootstrap it as well
            ids_and_addrs[CHAIN_LEN - 2].clone()
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // node 0 now tries to find the address of the very last node in the
    // chain; the chain should be long enough for it not to automatically
    // be connected to it after the bootstrap
    let found_addrs = nodes[0]
        .find_peer(ids_and_addrs[CHAIN_LEN - 1].0.clone())
        .await
        .unwrap();

    assert_eq!(found_addrs, vec![ids_and_addrs[CHAIN_LEN - 1].1.clone()]);
}

// TODO: split into separate, more advanced bootstrap and closest_peers
#[tokio::test(max_threads = 1)]
async fn kademlia_local_peer_discovery() {
    const BOOTSTRAPPER_COUNT: usize = 20;

    // start up PEER_COUNT bootstrapper nodes
    let mut bootstrappers = Vec::with_capacity(BOOTSTRAPPER_COUNT);
    for i in 0..BOOTSTRAPPER_COUNT {
        bootstrappers.push(Node::new(format!("bootstrapper_{}", i)).await);
    }

    // register the bootstrappers' ids and addresses
    let mut bootstrapper_ids: Vec<(PeerId, Vec<Multiaddr>)> =
        Vec::with_capacity(BOOTSTRAPPER_COUNT);
    for bootstrapper in &bootstrappers {
        let (id, mut addrs) = bootstrapper.identity().await.unwrap();
        let id = PeerId::from_public_key(id);
        // remove Protocol::P2p from the addrs
        for addr in &mut addrs {
            assert!(matches!(addr.pop(), Some(Protocol::P2p(_))));
        }

        bootstrapper_ids.push((id, addrs));
    }

    // connect all the bootstrappers to one another
    for (i, (node_id, _)) in bootstrapper_ids.iter().enumerate() {
        for (bootstrapper_id, addrs) in bootstrapper_ids
            .iter()
            .filter(|(peer_id, _)| peer_id != node_id)
        {
            bootstrappers[i]
                .add_peer(bootstrapper_id.clone(), addrs[0].clone())
                .await
                .unwrap();
        }
    }

    // introduce a peer and connect it to one of the bootstrappers
    let peer = Node::new("peer").await;
    assert!(peer
        .add_peer(
            bootstrapper_ids[0].0.clone(),
            bootstrapper_ids[0].1[0].clone()
        )
        .await
        .is_ok());

    // check that kad::bootstrap works
    assert!(peer.bootstrap().await.is_ok());

    // check that kad::get_closest_peers works
    assert!(peer.get_closest_peers().await.is_ok());
}

#[ignore = "targets an actual bootstrapper, so random failures can happen"]
#[tokio::test(max_threads = 1)]
async fn kademlia_popular_content_discovery() {
    let (bootstrapper_id, bootstrapper_addr): (PeerId, Multiaddr) = (
        "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
            .parse()
            .unwrap(),
        "/ip4/104.131.131.82/tcp/4001".parse().unwrap(),
    );

    // introduce a peer and specify the Kademlia protocol to it
    // without a specified protocol, the test will not complete
    let mut opts = IpfsOptions::inmemory_with_generated_keys();
    opts.kad_protocol = Some("/ipfs/lan/kad/1.0.0".to_owned());
    let peer = Node::with_options(opts).await;

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
