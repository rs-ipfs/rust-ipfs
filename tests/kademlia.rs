use async_std::future::timeout;
use cid::Cid;
use ipfs::{IpfsOptions, Node};
use libp2p::{Multiaddr, PeerId};
use std::time::Duration;

#[async_std::test]
async fn kademlia_local_peer_discovery() {
    const BOOTSTRAPPER_COUNT: usize = 20;

    // start up PEER_COUNT bootstrapper nodes
    let mut bootstrappers = Vec::with_capacity(BOOTSTRAPPER_COUNT);
    for i in 0..BOOTSTRAPPER_COUNT {
        bootstrappers.push(Node::new(format!("bootstrapper_{}", i)).await);
    }

    // register the bootstrappers' ids and addresses
    let mut bootstrapper_ids = Vec::with_capacity(BOOTSTRAPPER_COUNT);
    for bootstrapper in &bootstrappers {
        let (id, addrs) = bootstrapper.identity().await.unwrap();
        let id = PeerId::from_public_key(id);

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
#[async_std::test]
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
