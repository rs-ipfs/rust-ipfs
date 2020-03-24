use futures::join;
use ipfs::{Cid, Ipfs, IpfsOptions, TestTypes};
use libipld::dag::{DagPath, StoreDagExt};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = IpfsOptions::inmemory_with_generated_keys(true);
    let cid: Cid = "zdpuB1caPcm4QNXeegatVfLQ839Lmprd5zosXGwRUBJHwj66X".parse()?;
    let ipfs = Ipfs::new::<TestTypes>(options).await.unwrap();

    let path1 = DagPath::new(&cid, "0");
    let path2 = DagPath::new(&cid, "1");
    let f1 = ipfs.get(&path1);
    let f2 = ipfs.get(&path2);
    let (res1, res2) = join!(f1, f2);
    println!("Received block with contents: {:?}", res1?);
    println!("Received block with contents: {:?}", res2?);
    Ok(())
}
