fn main() {}

/*
use ipfs::{Ipfs, IpfsOptions, PeerId, TestTypes};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = IpfsOptions::inmemory_with_generated_keys(true);

    // Start daemon and initialize repo
    let ipfs = Ipfs::new::<TestTypes>(options).await?;

    // Create a Block
    let cid = ipfs.put_dag("block v0".into()).await.unwrap();
    let ipfs_path = IpfsPath::from(cid);
    // Publish a Block
    let ipns_path = ipfs.publish_ipns(&PeerId::random(), &ipfs_path).await?;

    // Resolve a Block
    let new_ipfs_path = ipfs.resolve_ipns(&ipns_path).await?;
    assert_eq!(ipfs_path, new_ipfs_path);

    // Resolve dnslink
    let ipfs_path = IpfsPath::from_str("/ipns/ipfs.io")?;
    println!("Resolving {:?}", ipfs_path.to_string());
    let ipfs_path = ipfs.resolve_ipns(&ipfs_path).await?;
    println!("Resolved stage 1: {:?}", ipfs_path.to_string());
    let ipfs_path = ipfs.resolve_ipns(&ipfs_path).await?;
    println!("Resolved stage 2: {:?}", ipfs_path.to_string());

    Ok(())
}*/
