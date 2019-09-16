use ipfs::{Ipfs, IpfsOptions, IpfsPath, PeerId, TestTypes};

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::run_async(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);
        ipfs.init_repo().await.unwrap();
        ipfs.open_repo().await.unwrap();

        // Create a Block
        let ipfs_path = ipfs.put_dag("block v0".into()).await.unwrap();
        // Publish a Block
        let ipns_path = ipfs.publish_ipns(&PeerId::random(), &ipfs_path).await.unwrap();

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

        ipfs.exit_daemon();
    });
}
