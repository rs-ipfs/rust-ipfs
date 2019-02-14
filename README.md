# Rust IPFS implementation
Currently implements an altruistic bitswap strategy over mdns.

## Known issues
* The mdns implementation of `go-ipfs` and `js-ipfs` aren't compatible with `rust-libp2p`.
* Locating providers via kademlia dht doesn't work.

## Getting started
```rust
use futures::prelude::*;
use ipfs::{Block, Ipfs};

fn main() {
    env_logger::Builder::new()
        .parse(&std::env::var("IPFS_LOG").unwrap_or_default())
        .init();
    let mut ipfs = Ipfs::new();
    let block = Block::from("hello block2\n");
    ipfs.put_block(block);
    let cid = Block::from("hello block\n").cid();
    let future = ipfs.get_block(cid).map(|block| {
        println!("Received block with contents: '{:?}'",
                 String::from_utf8_lossy(&block.data()));
    });
    tokio::run(ipfs.join(future).map(|_| ()));
}
```

## License
ISC License

Copyright (c) 2017, David Craven and others

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
