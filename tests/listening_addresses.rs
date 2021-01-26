#[tokio::test]
async fn multiple_consecutive_ephemeral_listening_addresses() {
    let node = ipfs::Node::new("test_node").await;

    let target = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));

    let first = node.add_listening_address(target.clone()).await.unwrap();
    assert_ne!(target, first);

    let second = node.add_listening_address(target.clone()).await.unwrap();
    assert_ne!(target, second);
    assert_ne!(first, second);
}

#[tokio::test]
async fn multiple_concurrent_ephemeral_listening_addresses_on_same_ip() {
    let node = ipfs::Node::new("test_node").await;

    let target = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));

    let first = node.add_listening_address(target.clone());
    let second = node.add_listening_address(target);

    let (first, second) = futures::future::join(first, second).await;

    // before we have an Swarm-alike api on the background task to make sure the two futures
    // (first and second) would attempt to modify the background task before a poll to the
    // inner swarm, this will produce one or two successes.
    //
    // with two attempts without polling the swarm in the between:
    // assert_eq!(first.is_ok(), second.is_err());
    //
    // intuitively it could seem that first will always succeed because it must get the first
    // attempt to push messages into the queue but not sure if that should be leaned on.

    assert!(
        first.is_ok() || second.is_ok(),
        "first: {:?}, second: {:?}",
        first,
        second
    );
}

#[tokio::test]
#[cfg(not(target_os = "macos"))]
async fn multiple_concurrent_ephemeral_listening_addresses_on_different_ip() {
    let node = ipfs::Node::new("test_node").await;

    // it doesnt work on mac os x as 127.0.0.2 is not enabled by default.
    let first =
        node.add_listening_address(libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16)));
    let second =
        node.add_listening_address(libp2p::build_multiaddr!(Ip4([127, 0, 0, 2]), Tcp(0u16)));

    let (first, second) = futures::future::join(first, second).await;

    // both should succeed
    first.unwrap();
    second.unwrap();
}

#[tokio::test]
async fn adding_unspecified_addr_resolves_with_first() {
    let node = ipfs::Node::new("test_node").await;
    // there is no test in trying to match this with others as ... that would be quite
    // perilous.
    node.add_listening_address(libp2p::build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16)))
        .await
        .unwrap();
}

#[tokio::test]
async fn listening_for_multiple_unspecified_addresses() {
    let node = ipfs::Node::new("test_node").await;
    // there is no test in trying to match this with others as ... that would be quite
    // perilous.
    let target = libp2p::build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16));
    let first = node.add_listening_address(target.clone());
    let second = node.add_listening_address(target);

    let (first, second) = futures::future::join(first, second).await;

    // this test is a bad one similar to multiple_concurrent_ephemeral_listening_addresses_on_same_ip
    // see also https://github.com/rs-ipfs/rust-ipfs/issues/194 for more discussion.

    // the other should be denied because there is a pending incomplete when trying to listen
    // on unspecified address
    assert!(
        first.is_ok() || second.is_ok(),
        "first: {:?}, second: {:?}",
        first,
        second
    );
}

#[tokio::test]
async fn remove_listening_address() {
    let node = ipfs::Node::new("test_node").await;

    let unbound = libp2p::build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16));
    let first = node.add_listening_address(unbound.clone()).await.unwrap();

    // the book keeping changes from matching the unbound address to the bound one returned
    // from the future.
    node.remove_listening_address(unbound.clone())
        .await
        .unwrap_err();
    node.remove_listening_address(first).await.unwrap();
}

#[test]
#[ignore]
fn remove_listening_address_before_completing() {
    // TODO: cannot test this before we have a way of getting between the queue used to communicate
    // with the IpfsFuture (or better yet, construct one ourselves here in the test) to make sure
    // we can push a IpfsEvent::AddListenerAddress followed by an IpfsEvent::RemoveListenerAddress
    // "immediatedly".
}

#[tokio::test]
async fn pre_configured_listening_addrs() {
    use ipfs::{IpfsOptions, MultiaddrWithPeerId, MultiaddrWithoutPeerId, Node};
    use libp2p::Multiaddr;
    use std::convert::TryFrom;

    let mut opts = IpfsOptions::inmemory_with_generated_keys();
    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
    opts.listening_addrs.push(addr.clone());
    let ipfs = Node::with_options(opts).await;

    let (_id, addrs) = ipfs.identity().await.unwrap();
    let addrs: Vec<MultiaddrWithoutPeerId> = addrs
        .into_iter()
        .map(|addr| MultiaddrWithPeerId::try_from(addr).unwrap().multiaddr)
        .collect();
    let addr = MultiaddrWithoutPeerId::try_from(addr).unwrap();

    assert!(
        addrs.contains(&addr),
        "pre-configured listening addr not found; listening addrs: {:?}",
        addrs
    );
}
