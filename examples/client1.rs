#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, Ipld, Types};
use futures::join;

fn main() {
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::run_async(async move {
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        let block1: Ipld = "block1".to_string().into();
        let block2: Ipld = "block2".to_string().into();
        let f1 = ipfs.put_dag(block1);
        let f2 = ipfs.put_dag(block2);
        let (res1, res2) = join!(f1, f2);

        let root: Ipld = vec![res1.unwrap(), res2.unwrap()].into();
        await!(ipfs.put_dag(root)).unwrap();
    });
}
