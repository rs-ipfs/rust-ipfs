<h1>
  <img src="https://ipfs.io/ipfs/QmRcFsCvTgGrB52UGpp9P2bSDmnYNTAATdRf4NBj8SKf77/rust-ipfs-logo-256w.png" width="128" /><br />
  Rust IPFS
</h1>

> The Interplanetary File System (IPFS), implemented in Rust

[![Financial Contributors on Open Collective](https://opencollective.com/rs-ipfs/all/badge.svg?label=financial+contributors)](https://opencollective.com/rs-ipfs) [![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) [![Back on OpenCollective](https://img.shields.io/badge/open%20collective-donate-yellow.svg)](https://opencollective.com/rs-ipfs) [![Matrix](https://img.shields.io/badge/matrix-%23rust_ipfs%3Amatrix.org-blue.svg)](https://riot.im/app/#/room/#rust-ipfs:matrix.org) [![Discord](https://img.shields.io/discord/475789330380488707?color=blueviolet&label=discord)](https://discord.gg/9E5SFvW)

## Table of Contents

- [Description](#description)
    - [Project Status](#project-status---pre-alpha)
    - [You can help](#you-can-help)
    - [What is IPFS?](#what-is-ipfs)
- [Install](#install)
- [Getting Started](#getting-started)
- [Roadmap](#roadmap)
- [Maintainers](#maintainers)
- [Alternatives](#alternatives-and-other-cool-related-projects)
- [Contributing](#contributing)
- [License](#license)
- [Trademarks](#trademarks)

## Description

This repository contains the crates for the IPFS core implementation which includes a blockstore, a libp2p integration which includes DHT content discovery and pubsub support, and HTTP API bindings. Our goal is to leverage both the unique properties of Rust to create powerful, performant software that works even in resource-constrained environments, while also maximizing interoperability with the other "flavors" of IPFS, namely JavaScript and Go.

### Project Status - `Alpha`

You can see details about what's implemented, what's not, and also learn about other ecosystem projects, at [Are We IPFS Yet?](https://areweipfsyet.rs)

For more information about IPFS see: https://docs.ipfs.io/introduction/overview/

## Install

Rust IPFS depends on `protoc` and `openssl`.

### Dependencies

First, install the dependencies.

With apt:

```bash
# apt-get install protobuf-compiler libssl-dev zlib1g-dev
```

With yum

```bash
# yum install protobuf-compiler libssl-dev zlib1g-dev
```

### Install `rust-ipfs` itself

The `rust-ipfs` binaries can be built from source. Our goal is to always be compatible with the **stable** release of Rust.

```bash
$ git clone https://github.com/rs-ipfs/rust-ipfs && cd rust-ipfs
$ cargo build --workspace
```

You will then find the binaries inside of the project root's `/target/debug` folder.

_Note: binaries available via `cargo install` is coming soon._

## Getting started
```rust,no_run
use tokio::task;
use futures::join;
use ipfs::{make_ipld, Ipfs, IpfsPath, Ipld, Types, UninitializedIpfs};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Start daemon and initialize repo
    let (ipfs, fut): (Ipfs<Types>, _) = UninitializedIpfs::default().await.start().await.unwrap();
    task::spawn(fut);

    // Create a DAG
    let f1 = ipfs.put_dag(make_ipld!("block1"));
    let f2 = ipfs.put_dag(make_ipld!("block2"));
    let (res1, res2) = join!(f1, f2);
    let root = make_ipld!([res1.unwrap(), res2.unwrap()]);
    let cid = ipfs.put_dag(root).await.unwrap();
    let path = IpfsPath::from(cid);

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
}
```

More usage examples coming soon :+1:

## Roadmap

Special thanks to the Web3 Foundation and Protocol Labs for their devgrant support.

### Completed Work

* Project Setup
* Testing Setup
    * Conformance testing
* HTTP API Scaffolding
* UnixFS Support
* `/pubsub/{publish,subscribe,peers,ls}`
* `/swarm/{connect,peers,addrs,addrs/local,disconnect}`
* `/id`
* `/version`
* `/shutdown`
* `/block/{get,put,rm,stat}`
* `/dag/{put,resolve}`
* `/refs` and `/refs/local`
* `/bitswap/{stat,wantlist}`
* `/cat`
* `/get`

### Work in Progress
* `/add`
- `/pin`
* DHT + Swarming

### Work still required
- Interop testing
- [/bootstrap](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/3)
- [/dht](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/4)
- [/name](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/5)
- [/ping](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/6)
- [/key](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/7)
- [/config](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/8)
- [/stats](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/9)
- [/files](https://github.com/rs-ipfs/ipfs-rust-conformance/issues/10) (regular and mfs)
- a few other miscellaneous endpoints not enumerated here

## Maintainers

Rust IPFS was originally authored by @dvc94ch and now actively maintained by @koivunej, and @aphelionz. Special thanks is given to [Protocol Labs](https://github.com/protocol), [Equilibrium](https://github.com/eqlabs), and [MRH.io](https://mrh.io).

## Alternatives and other cool, related projects

It’s been noted that the Rust-IPFS name and popularity may serve its organization from a "first-mover" perspective. However, alternatives with different philosophies do exist, and we believe that supporting a diverse IPFS community is important and will ultimately help produce the best solution possible.

- [`rust-ipfs-api`](https://github.com/ferristseng/rust-ipfs-api) - A Rust client for an existing IPFS HTTP API. Supports both hyper and actix.
- [`ipfs-embed`](https://github.com/ipfs-rust/ipfs-embed/) - An implementation based on [`sled`](https://github.com/ipfs-rust/ipfs-embed/)
- [`rust-ipld`](https://github.com/ipfs-rust/rust-ipld) - Basic rust ipld library supporting `dag-cbor`, `dag-json` and `dag-pb` formats.
- PolkaX's own [`rust-ipfs`](https://github.com/PolkaX/rust-ipfs)
- Parity's [`rust-libp2p`](https://github.com/libp2p/rust-libp2p), which does a lot the of heavy lifting here

If you know of another implementation or another cool project adjacent to these efforts, let us know!

### Contributing

See [the contributing docs](./CONTRIBUTING.md) for more info.

You can also back the project financially by reaching out or by becoming a backer on [OpenCollective](https://opencollective.com/rs-ipfs)

## Contributors

### Code Contributors

This project exists thanks to all the people who contribute. [[Contribute](CONTRIBUTING.md)].
<a href="https://github.com/rs-ipfs/rust-ipfs/graphs/contributors"><img src="https://opencollective.com/rs-ipfs/contributors.svg?width=890&button=false" /></a>

### Financial Contributors

Become a financial contributor and help us sustain our community. [[Contribute](https://opencollective.com/rs-ipfs/contribute)]

#### Individuals

<a href="https://opencollective.com/rs-ipfs"><img src="https://opencollective.com/rs-ipfs/individuals.svg?width=890"></a>

#### Organizations

Support this project with your organization. Your logo will show up here with a link to your website. [[Contribute](https://opencollective.com/rs-ipfs/contribute)]

<a href="https://opencollective.com/rs-ipfs/organization/0/website"><img src="https://opencollective.com/rs-ipfs/organization/0/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/1/website"><img src="https://opencollective.com/rs-ipfs/organization/1/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/2/website"><img src="https://opencollective.com/rs-ipfs/organization/2/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/3/website"><img src="https://opencollective.com/rs-ipfs/organization/3/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/4/website"><img src="https://opencollective.com/rs-ipfs/organization/4/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/5/website"><img src="https://opencollective.com/rs-ipfs/organization/5/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/6/website"><img src="https://opencollective.com/rs-ipfs/organization/6/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/7/website"><img src="https://opencollective.com/rs-ipfs/organization/7/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/8/website"><img src="https://opencollective.com/rs-ipfs/organization/8/avatar.svg"></a>
<a href="https://opencollective.com/rs-ipfs/organization/9/website"><img src="https://opencollective.com/rs-ipfs/organization/9/avatar.svg"></a>

## License

Dual licensed under MIT or Apache License (Version 2.0). See [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE) for more details.

## Trademarks

The [Rust logo and wordmark](https://www.rust-lang.org/policies/media-guide) are trademarks owned and protected by the [Mozilla Foundation](https://mozilla.org). The Rust and Cargo logos (bitmap and vector) are owned by Mozilla and distributed under the terms of the [Creative Commons Attribution license (CC-BY)](https://creativecommons.org/licenses/by/4.0/).
