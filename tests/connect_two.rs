use async_std::task;
use ipfs::{Ipfs, IpfsOptions, TestTypes};

/// Make sure two instances of ipfs can be connected.
#[async_std::test]
async fn connect_two_nodes() {
    // env_logger::init();

    // make sure the connection will only happen through explicit connect
    let mdns = false;

    let (tx, rx) = futures::channel::oneshot::channel();

    let node_a = task::spawn(async move {
        let opts = ipfs::IpfsOptions::inmemory_with_generated_keys(mdns);
        let ipfs = Ipfs::new::<TestTypes>(opts).await.unwrap();

        let (pk, addrs) = ipfs
            .identity()
            .await
            .expect("failed to read identity() on node_a");
        assert!(!addrs.is_empty());
        tx.send((pk, addrs, ipfs)).unwrap();
    });

    let (other_pk, other_addrs, _other_ipfs) = rx.await.unwrap();
    println!("got back from the other node: {:?}", other_addrs);

    let opts = IpfsOptions::inmemory_with_generated_keys(mdns);
    let ipfs = Ipfs::new::<TestTypes>(opts).await.unwrap();

    let _other_peerid = other_pk.into_peer_id();

    let mut connected = None;

    for addr in other_addrs {
        println!("trying {}", addr);
        match ipfs.connect(addr.clone()).await {
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

    node_a.await;
}
