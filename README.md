# Rust IPFS implementation
Currently implements an altruistic bitswap strategy over mdns.

## Getting started
```rust
#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, Ipld, IpldPath, Types};
use ipfs::{tokio_run, tokio_spawn};
use futures::join;

fn main() {
    let options = IpfsOptions::new();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);

    tokio_run(async move {
        // Start daemon and initialize repo
        tokio_spawn(ipfs.start_daemon());
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
```

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
