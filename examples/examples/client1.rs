use async_std::task;
use futures::join;
use ipfs::{IpfsOptions, Types, UninitializedIpfs};
use libipld::ipld;

fn main() {
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new()
        .parse_filters(&options.ipfs_log)
        .init();

    task::block_on(async move {
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        let f1 = ipfs.put_dag(ipld!("block1"));
        let f2 = ipfs.put_dag(ipld!("block2"));
        let (res1, res2) = join!(f1, f2);

        let root = ipld!([res1.unwrap(), res2.unwrap()]);
        ipfs.put_dag(root).await.unwrap();

        ipfs.exit_daemon().await;
    });
}
