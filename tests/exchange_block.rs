use cid::{Cid, Codec};
use ipfs::{Block, Node};
use multihash::Sha2_256;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test(max_threads = 1)]
async fn exchange_block() {
    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let a = Node::new("a").await;
    let b = Node::new("b").await;

    let (_, mut addrs) = b.identity().await.unwrap();

    a.connect(addrs.pop().expect("b must have address to connect to"))
        .await
        .unwrap();

    a.put_block(Block {
        cid: cid.clone(),
        data: data.clone(),
    })
    .await
    .unwrap();

    let f = timeout(Duration::from_secs(10), b.get_block(&cid));

    let Block { data: data2, .. } = f
        .await
        .expect("get_block did not complete in time")
        .unwrap();

    assert_eq!(data, data2);
}
