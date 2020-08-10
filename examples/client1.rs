use async_std::task;
use futures::join;
use ipfs::{Ipfs, TestTypes, UninitializedIpfs};
use libipld::ipld;

fn main() {
    tracing_subscriber::fmt::init();

    task::block_on(async move {
        let (ipfs, fut): (Ipfs<TestTypes>, _) =
            UninitializedIpfs::default().await.start().await.unwrap();
        task::spawn(fut);

        let f1 = ipfs.put_dag(ipld!("block1"));
        let f2 = ipfs.put_dag(ipld!("block2"));
        let (res1, res2) = join!(f1, f2);

        let root = ipld!([res1.unwrap(), res2.unwrap()]);
        ipfs.put_dag(root).await.unwrap();

        ipfs.exit_daemon().await;
    });
}
