use ipfs::{Block, Node};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;

fn filter(i: usize) -> bool {
    i % 2 == 0
}

#[async_std::test]
async fn bitswap_stress_test() {
    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    const NODE_COUNT: usize = 3;

    let mut nodes = Vec::with_capacity(NODE_COUNT);
    for i in 0..NODE_COUNT {
        nodes.push(Node::new(i.to_string()).await);
    }

    for i in 0..NODE_COUNT {
        for (j, peer) in nodes.iter().enumerate() {
            if i != j {
                let (_, mut addrs) = peer.identity().await.unwrap();
                nodes[i].connect(addrs.pop().unwrap()).await.unwrap();
            }
        }
    }

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
