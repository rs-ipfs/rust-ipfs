use ipfs::{Ipfs, IpfsOptions, Ipld, Types};
use futures::join;
use futures::{FutureExt, TryFutureExt};

fn main() {
    unimplemented!();
    /*
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::runtime::current_thread::block_on_all(async move {
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn(fut.unit_error().boxed().compat());
        ipfs.init_repo().await.unwrap();
        ipfs.open_repo().await.unwrap();

        let block1: Ipld = "block1".to_string().into();
        let block2: Ipld = "block2".to_string().into();
        let f1 = ipfs.put_dag(block1);
        let f2 = ipfs.put_dag(block2);
        let (res1, res2) = join!(f1, f2);

        let root: Ipld = vec![res1.unwrap(), res2.unwrap()].into();
        ipfs.put_dag(root).await.unwrap();
    }.unit_error().boxed().compat()).unwrap();
    */
}
