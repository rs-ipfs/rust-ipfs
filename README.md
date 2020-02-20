# Rust IPFS implementation
[![Build Status](https://travis-ci.org/dvc94ch/rust-ipfs.svg?branch=master)](https://travis-ci.org/dvc94ch/rust-ipfs)
[![Back on OpenCollective](https://img.shields.io/badge/open%20collective-donate-yellow.svg)](https://opencollective.com/ipfs-rust)

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
ISC License

Copyright (c) 2019, David Craven and others

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
