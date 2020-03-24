use futures::join;
use ipfs::{Ipfs, IpfsOptions, Types};
use libipld::hash::Sha2_256;
use libipld::store::StoreCborExt;
use libipld::DagCbor;

// TODO impl WriteCbor for String
#[derive(DagCbor)]
struct Message(String);

impl Message {
    pub fn new(msg: &str) -> Self {
        Self(msg.to_string())
    }
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = IpfsOptions::from_env()?;

    let ipfs = Ipfs::new::<Types>(options).await?;

    let block1 = Message::new("block1");
    let block2 = Message::new("block2");
    let f1 = ipfs.write_cbor::<Sha2_256, _>(&block1);
    let f2 = ipfs.write_cbor::<Sha2_256, _>(&block2);
    let (res1, res2) = join!(f1, f2);

    ipfs.write_cbor::<Sha2_256, _>(&vec![res1?, res2?]).await?;
    Ok(())
}
