use cid::{Cid, Codec};
use ipfs::Block;
use multihash::Sha2_256;
use std::time::Duration;
use tokio::time::timeout;

mod common;
use common::two_connected_nodes;

#[tokio::test(max_threads = 1)]
async fn exchange_block() {
    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let ((a, _), (b, _)) = two_connected_nodes().await;

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
