pub mod interop;

use ipfs::{Node, PeerId};

#[allow(dead_code)]
pub async fn two_connected_nodes() -> ((Node, PeerId), (Node, PeerId)) {
    let a = Node::new("a").await;
    let b = Node::new("b").await;

    let (a_pk, _) = a.identity().await.unwrap();
    let a_id = a_pk.into_peer_id();

    let (b_pk, mut addrs) = b.identity().await.unwrap();
    let b_id = b_pk.into_peer_id();

    a.connect(addrs.pop().expect("b must have address to connect to"))
        .await
        .unwrap();

    ((a, a_id), (b, b_id))
}
