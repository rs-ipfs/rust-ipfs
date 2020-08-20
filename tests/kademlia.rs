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
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;
    let node_c = Node::new("c").await;

    let (b_id, b_addrs) = node_b.identity().await.unwrap();
    let b_id = b_id.into_peer_id();
    let (c_id, mut c_addrs) = node_c.identity().await.unwrap();
    let c_id = c_id.into_peer_id();

    // register the nodes' addresses so they can bootstrap against
    // one another; at this point node_a is not aware of node_c's
    // existence
    node_a
        .add_peer(b_id.clone(), strip_peer_id(b_addrs[0].clone()))
        .await
        .unwrap();
    node_b
        .add_peer(c_id.clone(), strip_peer_id(c_addrs[0].clone()))
        .await
        .unwrap();
    node_c
        .add_peer(b_id, strip_peer_id(b_addrs[0].clone()))
        .await
        .unwrap();

    node_a.bootstrap().await.unwrap();
    node_b.bootstrap().await.unwrap();
    node_c.bootstrap().await.unwrap();

    // after the Kademlia bootstrap node_a is auto-connected to node_c, so
    // disconnect it in order to remove its addresses from the ones known
    // outside of the DHT.
    node_a
        .disconnect(c_addrs[0].clone().try_into().unwrap())
        .await
        .unwrap();

    let found_addrs = node_a.find_peer(c_id).await.unwrap();

    // remove Protocol::P2p from c_addrs
    for addr in &mut c_addrs {
        assert!(matches!(addr.pop(), Some(Protocol::P2p(_))));
    }

    assert_eq!(found_addrs, c_addrs);
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
