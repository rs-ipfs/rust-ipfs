# Rust IPFS implementation
[![Build Status](https://travis-ci.org/dvc94ch/rust-ipfs.svg?branch=master)](https://travis-ci.org/dvc94ch/rust-ipfs)

Currently implements an altruistic bitswap strategy over mdns.

## Getting started
```rust,no-run
use ipfs::{UninitializedIpfs, IpfsOptions, Ipld, Types};
use futures::join;
use futures::{FutureExt, TryFutureExt};

fn main() {
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse_filters(&options.ipfs_log).init();

    tokio::runtime::current_thread::block_on_all(async move {
        // Start daemon and initialize repo
        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        tokio::spawn(fut.unit_error().boxed().compat());

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
}
```

Note: `rust-ipfs` currently requires nightly, see `rust-toolchain` and `.travis.yml` for the tested version.

## License

Dual licensed under MIT or Apache License (Version 2.0). See [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE) for more details.
