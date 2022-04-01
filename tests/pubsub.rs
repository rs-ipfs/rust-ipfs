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
async fn publish_between_two_nodes_single_topic() {
    use futures::stream::StreamExt;

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
        // first node should witness it's the message it sent
        (&[topic.clone()], nodes[0].id, b"foobar", nodes[0].id),
        // second node should witness first nodes message, and so on.
        (&[topic.clone()], nodes[0].id, b"foobar", nodes[1].id),
        (&[topic.clone()], nodes[1].id, b"barfoo", nodes[0].id),
        (&[topic.clone()], nodes[1].id, b"barfoo", nodes[1].id),
    ]
    .iter()
    .cloned()
    .map(|(topics, sender, data, witness)| (topics.to_vec(), sender, data.to_vec(), witness))
    .collect::<Vec<_>>();

    let mut actual = Vec::new();

    for (st, own_peer_id) in &mut [
        (b_msgs.by_ref(), nodes[1].id),
        (a_msgs.by_ref(), nodes[0].id),
    ] {
        let received = timeout(
            Duration::from_secs(2),
            st.take(2)
                // Arc::try_unwrap will fail sometimes here as the sender side in src/p2p/pubsub.rs:305
                // can still be looping
                .map(|msg| (*msg).clone())
                .map(|msg| (msg.topics, msg.source, msg.data, *own_peer_id))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();

        actual.extend(received);
    }

    // sort the received messages both in expected and actual to make sure they are comparable;
    // order of receiving is not part of the tuple and shouldn't matter.
    let mut expected = expected;
    expected.sort_unstable();
    actual.sort_unstable();

    assert_eq!(
        actual, expected,
        "sent and received messages must be present on both nodes' streams"
    );

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

#[tokio::test]
async fn publish_between_two_nodes_different_topics() {
    use futures::stream::StreamExt;

    let nodes = spawn_nodes(2, Topology::Line).await;
    let node_a = &nodes[0];
    let node_b = &nodes[1];

    let topic_a = "shared-a".to_owned();
    let topic_b = "shared-b".to_owned();

    // Node A subscribes to Topic B
    // Node B subscribes to Topic A
    let mut a_msgs = node_a.pubsub_subscribe(topic_b.clone()).await.unwrap();
    let mut b_msgs = node_b.pubsub_subscribe(topic_a.clone()).await.unwrap();

    // need to wait to see both sides so that the messages will get through
    let mut appeared = false;
    for _ in 0..100usize {
        if node_a
            .pubsub_peers(Some(topic_a.clone()))
            .await
            .unwrap()
            .contains(&node_b.id)
            && node_b
                .pubsub_peers(Some(topic_b.clone()))
                .await
                .unwrap()
                .contains(&node_a.id)
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

    // Each node publishes to their own topic
    node_a
        .pubsub_publish(topic_a.clone(), b"foobar".to_vec())
        .await
        .unwrap();
    node_b
        .pubsub_publish(topic_b.clone(), b"barfoo".to_vec())
        .await
        .unwrap();

    // the order between messages is not defined, but both should see the other's message. since we
    // receive messages first from node_b's stream we expect this order.
    //
    // in this test case the nodes are not expected to see their own message because nodes are not
    // subscribing to the streams they are sending to.
    let expected = [
        (&[topic_a.clone()], node_a.id, b"foobar", node_b.id),
        (&[topic_b.clone()], node_b.id, b"barfoo", node_a.id),
    ]
    .iter()
    .cloned()
    .map(|(topics, sender, data, witness)| (topics.to_vec(), sender, data.to_vec(), witness))
    .collect::<Vec<_>>();

    let mut actual = Vec::new();
    for (st, own_peer_id) in &mut [(b_msgs.by_ref(), node_b.id), (a_msgs.by_ref(), node_a.id)] {
        let received = timeout(
            Duration::from_secs(2),
            st.take(1)
                .map(|msg| (*msg).clone())
                .map(|msg| (msg.topics, msg.source, msg.data, *own_peer_id))
                .next(),
        )
        .await
        .unwrap()
        .unwrap();
        actual.push(received);
    }

    // ordering is defined for expected and actual by the order of the looping above and the
    // initial expected creation.
    assert_eq!(expected, actual);

    drop(b_msgs);

    let mut disappeared = false;
    for _ in 0..100usize {
        if !node_a
            .pubsub_peers(Some(topic_a.clone()))
            .await
            .unwrap()
            .contains(&node_b.id)
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
