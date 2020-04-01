use ipfs::{Block, Node};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;

#[async_std::test]
async fn exchange_block() {
    env_logger::init();
    let mdns = false;

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let mut a = Node::new(mdns).await;
    let mut b = Node::new(mdns).await;

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

    let Block { data: data2, .. } = b.get_block(&cid).await.unwrap();

    assert_eq!(data, data2);
}
