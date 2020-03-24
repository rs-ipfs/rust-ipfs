use ipfs::{Ipfs, IpfsOptions, TestTypes};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = IpfsOptions::inmemory_with_generated_keys(true);
    let ipfs = Ipfs::new::<TestTypes>(options).await?;

    // Create a Block
    let data = b"block-provide".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    ipfs.put_block(cid, data).await?;

    // Retrive a Block
    let data = b"block-want\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    let data = ipfs.get_block(cid).await?;
    let contents = core::str::from_utf8(&data)?;
    println!("block contents: {:?}", contents);

    // Add a file
    // ipfs.add("./examples/block.data".into()).await.unwrap();

    // Get a file
    /*let path = "/QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW"
        .try_into()
        .unwrap();
    let file = ipfs.get(path).await.unwrap();
    let contents: String = file.into();
    println!("file contents: {:?}", contents);*/

    Ok(())
}
