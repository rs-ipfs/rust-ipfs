use async_std::task;

#[test]
fn multiple_consecutive_ephemeral_listening_addresses() {
    const MDNS: bool = false;
    task::block_on(async move {
        let node = ipfs::Node::new(MDNS).await;

        let target = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));

        let first = node.add_listening_address(target.clone()).await.unwrap();

        assert_ne!(target, first);

        let second = node.add_listening_address(target.clone()).await.unwrap();

        assert_ne!(target, second);

        assert_ne!(first, second);
    });
}

#[test]
fn multiple_concurrent_ephemeral_listening_addresses_on_same_ip() {
    const MDNS: bool = false;
    task::block_on(async move {
        let node = ipfs::Node::new(MDNS).await;

        let target = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));

        let first = node.add_listening_address(target.clone());
        let second = node.add_listening_address(target.clone());

        let (first, second) = futures::future::join(first, second).await;

        // one should succeed and other should fail
        assert_eq!(first.is_ok(), second.is_err());
    });
}

#[test]
fn multiple_concurrent_ephemeral_listening_addresses_on_different_ip() {
    const MDNS: bool = false;
    task::block_on(async move {
        let node = ipfs::Node::new(MDNS).await;

        // hopefully this will work on all platforms
        let first =
            node.add_listening_address(libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16)));
        let second =
            node.add_listening_address(libp2p::build_multiaddr!(Ip4([127, 0, 0, 2]), Tcp(0u16)));

        let (first, second) = futures::future::join(first, second).await;

        // both should succeed
        first.unwrap();
        second.unwrap();
    });
}

#[test]
fn remove_listening_address() {
    const MDNS: bool = false;
    task::block_on(async move {
        let node = ipfs::Node::new(MDNS).await;

        let unbound = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));
        let first = node.add_listening_address(unbound.clone()).await.unwrap();

        node.remove_listening_address(unbound.clone())
            .await
            .unwrap_err();
        node.remove_listening_address(first).await.unwrap();
    });
}

#[test]
#[ignore]
fn remove_listening_address_before_completing() {
    // TODO: cannot test this before we have a way of getting between the queue used to communicate
    // with the IpfsFuture (or better yet, construct one ourselves here in the test) to make sure
    // we can push a IpfsEvent::AddListenerAddress followed by an IpfsEvent::RemoveListenerAddress
    // "immediatedly".
}
