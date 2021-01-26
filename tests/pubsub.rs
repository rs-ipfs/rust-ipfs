use futures::future::pending;
use futures::stream::StreamExt;
use ipfs::Node;
use std::time::Duration;
use tokio::time::timeout;

mod common;
use common::{spawn_nodes, Topology};

#[tokio::test]
async fn subscribe_only_once() {
    let a = Node::new("test_node").await;
    let _stream = a.pubsub_subscribe("some_topic".into()).await.unwrap();
    a.pubsub_subscribe("some_topic".into()).await.unwrap_err();
}

#[tokio::test]
async fn resubscribe_after_unsubscribe() {
    let a = Node::new("test_node").await;

    let mut stream = a.pubsub_subscribe("topic".into()).await.unwrap();
    a.pubsub_unsubscribe("topic").await.unwrap();
    // sender has been dropped
    assert_eq!(stream.next().await, None);

    drop(a.pubsub_subscribe("topic".into()).await.unwrap());
}

#[tokio::test]
async fn unsubscribe_via_drop() {
    let a = Node::new("test_node").await;

    let msgs = a.pubsub_subscribe("topic".into()).await.unwrap();
    assert_eq!(a.pubsub_subscribed().await.unwrap(), &["topic"]);

    drop(msgs);

    let empty: &[&str] = &[];
    assert_eq!(a.pubsub_subscribed().await.unwrap(), empty);
}

#[tokio::test]
async fn can_publish_without_subscribing() {
    let a = Node::new("test_node").await;
    a.pubsub_publish("topic".into(), b"foobar".to_vec())
        .await
        .unwrap()
}

#[tokio::test]
#[allow(clippy::mutable_key_type)] // clippy doesn't like Vec inside HashSet
async fn publish_between_two_nodes() {
    use futures::stream::StreamExt;
    use std::collections::HashSet;

    let nodes = spawn_nodes(2, Topology::Line).await;

    let topic = "shared".to_owned();

    let mut a_msgs = nodes[0].pubsub_subscribe(topic.clone()).await.unwrap();
    let mut b_msgs = nodes[1].pubsub_subscribe(topic.clone()).await.unwrap();

    // need to wait to see both sides so that the messages will get through
    let mut appeared = false;
    for _ in 0..100usize {
        if nodes[0]
            .pubsub_peers(Some(topic.clone()))
            .await
            .unwrap()
            .contains(&nodes[1].id)
            && nodes[1]
                .pubsub_peers(Some(topic.clone()))
                .await
                .unwrap()
                .contains(&nodes[0].id)
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

    nodes[0]
        .pubsub_publish(topic.clone(), b"foobar".to_vec())
        .await
        .unwrap();
    nodes[1]
        .pubsub_publish(topic.clone(), b"barfoo".to_vec())
        .await
        .unwrap();

    // the order is not defined, but both should see the other's message and the message they sent
    let expected = [
        (&[topic.clone()], &nodes[0].id, b"foobar"),
        (&[topic.clone()], &nodes[1].id, b"barfoo"),
    ]
    .iter()
    .cloned()
    .map(|(topics, id, data)| (topics.to_vec(), *id, data.to_vec()))
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
        if !nodes[0]
            .pubsub_peers(Some(topic.clone()))
            .await
            .unwrap()
            .contains(&nodes[1].id)
        {
            disappeared = true;
            break;
        }
        timeout(Duration::from_millis(100), pending::<()>())
            .await
            .unwrap_err();
    }

    assert!(disappeared, "timed out before a saw b's unsubscription");
}

#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
#[tokio::test]
#[ignore = "doesn't work yet"]
async fn pubsub_interop() {
    use common::interop::{api_call, ForeignNode};
    use futures::{future, pin_mut};

    let rust_node = Node::new("rusty_boi").await;
    let foreign_node = ForeignNode::new();
    let foreign_api_port = foreign_node.api_port;

    rust_node
        .connect(foreign_node.addrs[0].clone())
        .await
        .unwrap();

    const TOPIC: &str = "shared";

    let _rust_sub_stream = rust_node.pubsub_subscribe(TOPIC.to_string()).await.unwrap();

    let foreign_sub_answer = future::maybe_done(api_call(
        foreign_api_port,
        format!("pubsub/sub?arg={}", TOPIC),
    ));
    pin_mut!(foreign_sub_answer);
    assert_eq!(foreign_sub_answer.as_mut().output_mut(), None);

    // need to wait to see both sides so that the messages will get through
    let mut appeared = false;
    for _ in 0..100usize {
        if rust_node
            .pubsub_peers(Some(TOPIC.to_string()))
            .await
            .unwrap()
            .contains(&foreign_node.id)
            && api_call(foreign_api_port, &format!("pubsub/peers?arg={}", TOPIC))
                .await
                .contains(&rust_node.id.to_string())
        {
            appeared = true;
            break;
        }
        timeout(Duration::from_millis(200), pending::<()>())
            .await
            .unwrap_err();
    }

    assert!(
        appeared,
        "timed out before both nodes appeared as pubsub peers"
    );
}
