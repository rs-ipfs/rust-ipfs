use ipfs::{Ipfs, IpfsOptions, TestTypes};

/// Make sure two instances of ipfs can be connected.
#[async_std::test]
async fn connect_two_nodes() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // make sure the connection will only happen through explicit connect
    let opts1 = IpfsOptions::inmemory_with_generated_keys(false);
    let ipfs1 = Ipfs::new::<TestTypes>(opts1).await?;
    let (_pk, addrs) = ipfs1.identity().await?;
    assert!(!addrs.is_empty());

    let opts2 = IpfsOptions::inmemory_with_generated_keys(false);
    let ipfs2 = Ipfs::new::<TestTypes>(opts2).await?;
    let mut connected = None;
    for addr in &addrs {
        println!("trying {}", addr);
        match ipfs2.connect(addr.clone()).await {
            Ok(_) => {
                connected = Some(addr);
                break;
            }
            Err(e) => {
                println!("Failed connecting to {}: {}", addr, e);
            }
        }
    }

    let connected = connected.expect("Failed to connect to anything");
    println!("connected to {}", connected);
    Ok(())
}
