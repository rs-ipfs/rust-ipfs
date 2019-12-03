use ipfs::{Ipfs, IpfsOptions, IpfsPath, TestTypes};
use futures::join;
use futures::{FutureExt, TryFutureExt};

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let path = IpfsPath::from_str("/ipfs/zdpuB1caPcm4QNXeegatVfLQ839Lmprd5zosXGwRUBJHwj66X").unwrap();

    tokio::runtime::current_thread::block_on_all(async move {
        let (ipfs, fut) = Ipfs::new(options).start().await.unwrap();
        tokio::spawn(fut.unit_error().boxed().compat());

        let f1 = ipfs.get_dag(path.sub_path("0").unwrap());
        let f2 = ipfs.get_dag(path.sub_path("1").unwrap());
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        ipfs.exit_daemon();
    }.unit_error().boxed().compat()).unwrap();
}
