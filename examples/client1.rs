use tokio::task;
use futures::join;
use ipfs::{make_ipld, Ipfs, TestTypes, UninitializedIpfs};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (ipfs, fut): (Ipfs<TestTypes>, _) =
        UninitializedIpfs::default().await.start().await.unwrap();
    task::spawn(fut);

    let f1 = ipfs.put_dag(make_ipld!("block1"));
    let f2 = ipfs.put_dag(make_ipld!("block2"));
    let (res1, res2) = join!(f1, f2);

    let root = make_ipld!([res1.unwrap(), res2.unwrap()]);
    ipfs.put_dag(root).await.unwrap();

    ipfs.exit_daemon().await;
}
