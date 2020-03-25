use ipfs::{Ipfs, IpfsOptions, TestTypes};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;

/// Discovers a peer via mdns and exchanges a block through bitswap.
#[async_std::test]
async fn exchange_block() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let opts1 = IpfsOptions::inmemory_with_generated_keys(true);
    let ipfs1 = Ipfs::new::<TestTypes>(opts1).await?;
    ipfs1.put_block(cid.clone(), data.clone()).await?;

    let opts2 = IpfsOptions::inmemory_with_generated_keys(true);
    let ipfs2 = Ipfs::new::<TestTypes>(opts2).await?;
    let data2 = ipfs2.get_block(cid).await?;

    assert_eq!(data, data2);
    Ok(())
}
