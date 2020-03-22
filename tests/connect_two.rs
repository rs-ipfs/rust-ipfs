use async_std::task;

/// Make sure two instances of ipfs can be connected.
#[test]
fn connect_two_nodes() {
    env_logger::init();

    // make sure the connection will only happen through explicit connect
    let mdns = false;

    let (tx, rx) = futures::channel::oneshot::channel();

    let node_a = task::spawn(async move {
        let opts = ipfs::IpfsOptions::inmemory_with_generated_keys(mdns);
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(opts).await.start().await.unwrap();

        let jh = task::spawn(fut);

        let (pk, addrs) = ipfs.identity().await.expect("failed to read identity() on node_a");
        tx.send((pk, addrs, ipfs, jh)).unwrap();
    });

    task::block_on(async move {
        let (other_pk, other_addrs, other_ipfs, other_jh) = rx.await.unwrap();

        log::debug!("got back from the other node: {:?}", other_addrs);
        assert!(!other_addrs.is_empty());

        let opts = ipfs::IpfsOptions::inmemory_with_generated_keys(mdns);
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(opts).await.start().await.unwrap();
        let jh = task::spawn(fut);

        let _other_peerid = other_pk.into_peer_id();

        let mut connected = None;

        for addr in other_addrs {
            log::debug!("trying {}", addr);
            match ipfs.connect(addr.clone()).await {
                Ok(_) => {
                    connected = Some(addr);
                    break;
                },
                Err(e) => {
                    log::warn!("Failed connecting to {}: {}", addr, e);
                }
            }
        }

        async_std::future::timeout(std::time::Duration::from_millis(10*1000), async_std::future::pending::<()>()).await.unwrap();

        let connected = connected.expect("Failed to connect to anything");
        log::debug!("connected to {}", connected);

        other_ipfs.exit_daemon().await;
        other_jh.await;
        node_a.await;

        ipfs.exit_daemon().await;
        jh.await;
    });
}

