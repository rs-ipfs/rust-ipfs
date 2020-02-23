use std::str::FromStr;
use ipfs::{UninitializedIpfs, IpfsOptions, IpfsPath, TestTypes};
use futures::join;
use async_std::task;

fn main() {
    let options = IpfsOptions::<TestTypes>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let path = IpfsPath::from_str("/ipfs/zdpuB1caPcm4QNXeegatVfLQ839Lmprd5zosXGwRUBJHwj66X").unwrap();

    task::block_on(async move {
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        let f1 = ipfs.get_dag(path.sub_path("0").unwrap());
        let f2 = ipfs.get_dag(path.sub_path("1").unwrap());
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        ipfs.exit_daemon();
    });
}
