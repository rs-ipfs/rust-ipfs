use async_std::task;
use ipfs::{IpfsOptions, IpfsPath, PeerId, TestTypes, UninitializedIpfs};
use std::str::FromStr;

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new()
        .parse_filters(&options.ipfs_log)
        .init();

    task::block_on(async move {
        // Start daemon and initialize repo
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        // Create a Block
        let cid = ipfs.put_dag("block v0".into()).await.unwrap();
        let ipfs_path = IpfsPath::from(cid);
        // Publish a Block
        let ipns_path = ipfs
            .publish_ipns(&PeerId::random(), &ipfs_path)
            .await
            .unwrap();

        // Resolve a Block
        let new_ipfs_path = ipfs.resolve_ipns(&ipns_path).await.unwrap();
        assert_eq!(ipfs_path, new_ipfs_path);

        // Resolve dnslink
        let ipfs_path = IpfsPath::from_str("/ipns/ipfs.io").unwrap();
        println!("Resolving {:?}", ipfs_path.to_string());
        let ipfs_path = ipfs.resolve_ipns(&ipfs_path).await.unwrap();
        println!("Resolved stage 1: {:?}", ipfs_path.to_string());
        let ipfs_path = ipfs.resolve_ipns(&ipfs_path).await.unwrap();
        println!("Resolved stage 2: {:?}", ipfs_path.to_string());

        ipfs.exit_daemon().await;
    });
}
