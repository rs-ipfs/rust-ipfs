#![feature(async_await, await_macro, futures_api)]
use ipfs::{Cid, IpldPath, Ipfs, IpfsOptions, TestTypes};
use futures::join;

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);
    let cid = Cid::from("zdpuB1caPcm4QNXeegatVfLQ839Lmprd5zosXGwRUBJHwj66X").unwrap();
    let path1 = IpldPath::from(cid.clone(), "0").unwrap();
    let path2 = IpldPath::from(cid, "1").unwrap();

    tokio::run_async(async move {
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);

        let f1 = ipfs.get_dag(path1);
        let f2 = ipfs.get_dag(path2);
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        ipfs.exit_daemon();
    });
}
