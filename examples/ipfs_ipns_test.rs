#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, PeerId, TestTypes};

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::run_async(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        // Create a Block
        let ipfs_path = await!(ipfs.put_dag("block v0".into())).unwrap();
        // Publish a Block
        let ipns_path = await!(ipfs.publish_ipns(&PeerId::random(), &ipfs_path)).unwrap();

        // Resolve a Block
        let new_ipfs_path = await!(ipfs.resolve_ipns(&ipns_path)).unwrap();
        assert_eq!(ipfs_path, new_ipfs_path);

        ipfs.exit_daemon();
    });
}
