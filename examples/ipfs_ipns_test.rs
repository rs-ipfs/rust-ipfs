use ipfs::{Ipfs, IpfsOptions, IpfsPath, PeerId, TestTypes};
use futures::{FutureExt, TryFutureExt};

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();

    tokio::runtime::current_thread::block_on_all(async move {
        // Start daemon and initialize repo
        let (ipfs, fut) = Ipfs::new(options).start().await.unwrap();
        tokio::spawn(fut.unit_error().boxed().compat());

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
    }.unit_error().boxed().compat()).unwrap();
}
