#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, Ipld, IpldPath, Types};
use futures::join;

fn main() {
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::run_async(async move {
        // Start daemon and initialize repo
        tokio::spawn_async(ipfs.start_daemon());
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();

        // Create a DAG
        let block1: Ipld = "block1".to_string().into();
        let block2: Ipld = "block2".to_string().into();
        let f1 = ipfs.put_dag(block1);
        let f2 = ipfs.put_dag(block2);
        let (res1, res2) = join!(f1, f2);
        let root: Ipld = vec![res1.unwrap(), res2.unwrap()].into();
        let root_cid = await!(ipfs.put_dag(root)).unwrap();

        // Query the DAG
        let path1 = IpldPath::from(root_cid.clone(), "0").unwrap();
        let path2 = IpldPath::from(root_cid, "1").unwrap();
        let f1 = ipfs.get_dag(path1);
        let f2 = ipfs.get_dag(path2);
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        // Exit
        ipfs.exit_daemon();
    });
}
