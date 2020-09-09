use cid::{Cid, Codec};
use ipfs::Block;
use multihash::Sha2_256;

mod common;
use common::{spawn_connected_nodes, Topology};

fn filter(i: usize) -> bool {
    i % 2 == 0
}

// this test is designed to trigger unfavorable conditions for the bitswap
// protocol by putting blocks in every second node and attempting to get
// them from the other nodes; intended to be used for debugging or stress
// testing the bitswap protocol (though it would be advised to uncomment
// the tracing_subscriber for stress-testing purposes)
#[ignore]
#[tokio::test(max_threads = 1)]
async fn bitswap_stress_test() {
    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let nodes = spawn_connected_nodes(5, Topology::Mesh).await;

    for (i, node) in nodes.iter().enumerate() {
        if filter(i) {
            node.put_block(Block {
                cid: cid.clone(),
                data: data.clone(),
            })
            .await
            .unwrap();
        }
    }

    for (i, node) in nodes.iter().enumerate() {
        if !filter(i) {
            node.get_block(&cid).await.unwrap();
        }
    }
}
