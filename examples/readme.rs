use ipfs::{Ipfs, IpfsOptions, Ipld, Types};
use futures::join;

fn main() {
    unimplemented!();
    /*
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();
    let mut ipfs = Ipfs::new(options);

    tokio::runtime::current_thread::block_on_all(async move {
        // Start daemon and initialize repo
        let fut = ipfs.start_daemon().unwrap();
        tokio::spawn_async(fut);
        ipfs.init_repo().await.unwrap();
        ipfs.open_repo().await.unwrap();

        // Create a DAG
        let block1: Ipld = "block1".to_string().into();
        let block2: Ipld = "block2".to_string().into();
        let f1 = ipfs.put_dag(block1);
        let f2 = ipfs.put_dag(block2);
        let (res1, res2) = join!(f1, f2);
        let root: Ipld = vec![res1.unwrap(), res2.unwrap()].into();
        let path = ipfs.put_dag(root).await.unwrap();

        // Query the DAG
        let path1 = path.sub_path("0").unwrap();
        let path2 = path.sub_path("1").unwrap();
        let f1 = ipfs.get_dag(path1);
        let f2 = ipfs.get_dag(path2);
        let (res1, res2) = join!(f1, f2);
        println!("Received block with contents: {:?}", res1.unwrap());
        println!("Received block with contents: {:?}", res2.unwrap());

        // Exit
        ipfs.exit_daemon();
    }.unit_error().boxed().compat()).unwrap();
    */
}
