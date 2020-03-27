use async_std::future::{pending, timeout};
use futures::stream::StreamExt;
use ipfs::{Node, PeerId};
use std::time::Duration;

// Disable mdns for these tests not to connect to any local go-ipfs node
const MDNS: bool = false;

#[async_std::test]
async fn subscribe_only_once() {
    let a = Node::new(MDNS).await;
    let _stream = a.pubsub_subscribe("some_topic").await.unwrap();
    a.pubsub_subscribe("some_topic").await.unwrap_err();
}

#[async_std::test]
async fn resubscribe_after_unsubscribe() {
    let a = Node::new(MDNS).await;

    let mut stream = a.pubsub_subscribe("topic").await.unwrap();
    a.pubsub_unsubscribe("topic").await.unwrap();
    // sender has been dropped
    assert_eq!(stream.next().await, None);

    drop(a.pubsub_subscribe("topic").await.unwrap());
}

#[async_std::test]
async fn unsubscribe_via_drop() {
    let a = Node::new(MDNS).await;

    let msgs = a.pubsub_subscribe("topic").await.unwrap();
    assert_eq!(a.pubsub_subscribed().await.unwrap(), &["topic"]);

    drop(msgs);

    let empty: &[&str] = &[];
    assert_eq!(a.pubsub_subscribed().await.unwrap(), empty);
}

#[async_std::test]
async fn can_publish_without_subscribing() {
    let a = Node::new(MDNS).await;
    a.pubsub_publish("topic", b"foobar").await.unwrap()
}

#[async_std::test]
async fn publish_between_two_nodes() {
    use futures::stream::StreamExt;
    use std::collections::HashSet;
    use std::sync::Arc;

    let ((a, a_id), (b, b_id)) = two_connected_nodes().await;

    let topic = "shared";

    let mut a_msgs = a.pubsub_subscribe(topic).await.unwrap();
    let mut b_msgs = b.pubsub_subscribe(topic).await.unwrap();

    // need to wait to see both sides so that the messages will get through
    let mut appeared = false;
    for _ in 0..100usize {
        if a.pubsub_peers(Some(topic)).await.unwrap().contains(&b_id)
            && b.pubsub_peers(Some(topic)).await.unwrap().contains(&a_id)
        {
            appeared = true;
            break;
        }
        timeout(Duration::from_millis(100), pending::<()>())
            .await
            .unwrap_err();
    }

    assert!(
        appeared,
        "timed out before both nodes appeared as pubsub peers"
    );

    a.pubsub_publish(topic, b"foobar").await.unwrap();
    b.pubsub_publish(topic, b"barfoo").await.unwrap();

    // the order is not defined, but both should see the other's message and the message they sent
    let expected = [(&[topic], &a_id, b"foobar"), (&[topic], &b_id, b"barfoo")]
        .iter()
        .cloned()
        .map(|(topics, id, data)| {
            (
                topics.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                id.clone(),
                data.to_vec(),
            )
        })
        .collect::<HashSet<_>>();

    for st in &mut [b_msgs.by_ref(), a_msgs.by_ref()] {
        let actual = st
            .take(2)
            // Arc::try_unwrap will fail sometimes here as the sender side in src/p2p/pubsub.rs:305
            // can still be looping
            .map(|msg| (*msg).clone())
            .map(|msg| (msg.topics, msg.source, msg.data))
            .collect::<HashSet<_>>()
            .await;
        assert_eq!(expected, actual);
    }

    drop(b_msgs);

    let mut disappeared = false;
    for _ in 0..100usize {
        if !a.pubsub_peers(Some(topic)).await.unwrap().contains(&b_id) {
            disappeared = true;
            break;
        }
        timeout(Duration::from_millis(100), pending::<()>())
            .await
            .unwrap_err();
    }

    assert!(disappeared, "timed out before a saw b's unsubscription");
}

async fn two_connected_nodes() -> ((Node, PeerId), (Node, PeerId)) {
    let a = Node::new(MDNS).await;
    let b = Node::new(MDNS).await;

    let (a_pk, _) = a.identity().await.unwrap();
    let a_id = a_pk.into_peer_id();

    let (b_pk, mut addrs) = b.identity().await.unwrap();
    let b_id = b_pk.into_peer_id();

    a.connect(addrs.pop().expect("b must have address to connect to"))
        .await
        .unwrap();

    ((a, a_id), (b, b_id))
}
