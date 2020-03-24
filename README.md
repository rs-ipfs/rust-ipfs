<h1>
  <img src="https://ipfs.io/ipfs/QmRcFsCvTgGrB52UGpp9P2bSDmnYNTAATdRf4NBj8SKf77/rust-ipfs-logo-256w.png" width="128" /><br />
  Rust IPFS
</h1>

> The Interplanetary File System (IPFS), implemented in Rust

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) [![Back on OpenCollective](https://img.shields.io/badge/open%20collective-donate-yellow.svg)](https://opencollective.com/ipfs-rust) [![Matrix](https://img.shields.io/badge/matrix-%23rust_ipfs%3Amatrix.org-blue.svg)](https://riot.im/app/#/room/#rust-ipfs:matrix.org) [![Discord](https://img.shields.io/discord/475789330380488707?color=blueviolet&label=discord)](https://discord.gg/9E5SFvW)


## Description

This repository contains the crates for the IPFS core implementation which includes a blockstore, libp2p integration, and HTTP API bindings. Our goal is to leverage both the unique properties of Rust to create powerful, performant software that works even in resource-constrained environments while maximizing interoperability with the other "flavors" of IPFS, namely JavaScript and Go.

### Project Status - `Pre-Alpha`

There's a lot of great work in here, and a lot more coming that isn't implemented yet. Recently, this project was awarded a [dev grant from Protocol Labs](https://github.com/ipfs/devgrants/tree/master/open-grants/ipfs-rust), empowering us to raise our level of conformance. After the grant work is complete the project will achieve alpha stage.

### You can help.

PRs and Issues accepted for any of the following. See [the contributing docs](./CONTRIBUTING.md) for more info.
* Implement endpoints not covered by the devgrant proposal. See the [roadmap section](#roadmap) below
* Back the project financially by reaching out or by becoming a backer on [OpenCollective](https://opencollective.com/ipfs-rust)

### What is IPFS?

IPFS is a global, versioned, peer-to-peer filesystem. It combines good ideas from previous systems such Git, BitTorrent, Kademlia, SFS, and the Web. It is like a single bittorrent swarm, exchanging git objects. IPFS provides an interface as simple as the HTTP web, but with permanence built in. You can also mount the world at /ipfs.

For more info see: https://docs.ipfs.io/introduction/overview/

## Table of Contents

- [Description](#description)
    - [Project Status](#project-status---pre-alpha)
    - [You can help](#you-can-help)
    - [What is IPFS?](#what-is-ipfs)
- [Install](#install)
- [Getting Started](#getting-started)
- [Roadmap](#roadmap)
- [Maintainers](#maintainers)
- [License](#license)
- [Trademarks](#trademarks)


## Install

The `rust-ipfs` binaries can be built from source. Our goal is to always be compatible with the **stable** release of Rust.

```bash
$ git clone https://github.com/ipfs-rust/rust-ipfs && cd rust-ipfs
$ cargo build --workspace
```

You will then find the binaries inside of the project root's `/target/debug` folder.

_Note: binaries available via `cargo install` is coming soon._

## Getting started
```rust,no_run
use futures::join;
use ipfs::{IpfsOptions, Ipfs, Types};
use libipld::dag::{DagPath, StoreDagExt};
use libipld::hash::Sha2_256;
use libipld::store::StoreCborExt;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = IpfsOptions::from_env()?;

    // Start daemon and initialize repo
    let ipfs = Ipfs::new::<Types>(options).await?;

    // Create a DAG
    let f1 = ipfs.write_cbor::<Sha2_256, _>(&1000);
    let f2 = ipfs.write_cbor::<Sha2_256, _>(&2000);
    let (res1, res2) = join!(f1, f2);
    let root = vec![res1?, res2?];
    let cid = ipfs.write_cbor::<Sha2_256, _>(&root).await?;

    // Query the DAG
    let path1 = DagPath::new(&cid, "0");
    let path2 = DagPath::new(&cid, "1");
    let f1 = ipfs.get(&path1);
    let f2 = ipfs.get(&path2);
    let (res1, res2) = join!(f1, f2);
    println!("Received block with contents: {:?}", res1?);
    println!("Received block with contents: {:?}", res2?);

    Ok(())
}
```

More usage examples coming soon :+1:

## Roadmap

A large portion of this work is covered by an [IPFS Devgrant from Protocol Labs](https://github.com/ipfs/devgrants/tree/master/open-grants/ipfs-rust). In the proposal, we discussed using implemented HTTP API endpoints as a metric to determine progress. _There are always opportunities for community members to contribute by helping out with endpoints not covered in the grant._

### Devgrant Phase 1.0

- Project Setup
- Testing Setup
    - Conformance testing
    - Interop testing
- HTTP API Scaffolding

### Devgrant Phase 1.1

- Blockstore implementation
- /pubsub
- /swarm
- /version
- /id

### Devgrant Phase 1.2

- Bitswap updates
- /block
- /dag
- /refs
- /bitswap

### Work not covered by the grant

- /object
- /pin
- /bootstrap
- /dht
- /name
- /ping
- /key
- /config
- /stats
- /files (regular and mfs)
- a few othrer miscellaneous endpoints as well not enumerated here

## Maintainers

Rust IPFS is currently actively maintained by @dvc94ch, @koivunej, and @aphelionz. Special thanks is given to [Protocol Labs](https://github.com/protocol), [Equilibrium Labs](https://github.com/eqlabs), and [MRH.io](https://mrh.io).

## License

Dual licensed under MIT or Apache License (Version 2.0). See [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE) for more details.

## Trademarks

The [Rust logo and wordmark](https://www.rust-lang.org/policies/media-guide) are trademarks owned and protected by the [Mozilla Foundation](https://mozilla.org). The Rust and Cargo logos (bitmap and vector) are owned by Mozilla and distributed under the terms of the [Creative Commons Attribution license (CC-BY)](https://creativecommons.org/licenses/by/4.0/).
