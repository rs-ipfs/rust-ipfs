#![feature(async_await, await_macro, futures_api)]
use ipfs::{Cid, IpldPath, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use ipfs::{tokio_run, tokio_spawn};
use futures::join;

#[derive(Clone)]
struct Types;

impl RepoTypes for Types {
    type TBlockStore = ipfs::repo::mem::MemBlockStore;
    type TDataStore = ipfs::repo::mem::MemDataStore;
}

impl SwarmTypes for Types {
    type TStrategy = ipfs::bitswap::strategy::AltruisticStrategy<Self>;
}

impl IpfsTypes for Types {}

fn main() {
    let options = IpfsOptions::test();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);
    let cid = Cid::from("zdpuB1caPcm4QNXeegatVfLQ839Lmprd5zosXGwRUBJHwj66X").unwrap();
    let path1 = IpldPath::from(cid.clone(), "0").unwrap();
    let path2 = IpldPath::from(cid, "1").unwrap();

    tokio_run(async move {
        tokio_spawn(ipfs.start_daemon());

        let f1 = ipfs.get_dag(path1);
        let f2 = ipfs.get_dag(path2);
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        ipfs.exit_daemon();
    });
}
