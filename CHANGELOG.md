# Next

* feat: introduce repo locking [#429]
* ci: update go-ipfs to `0.7.0` for interop tests [#428]
* refactor(http): introduce `Config` as the facade for configuration [#423]
* feat(http): create `Profile` abstraction [#421]
* feat: `sled` pinstore [#439], [#442], [#444]
* chore: update a lot of dependencies including libp2p, tokio, warp [#446]
* fix: rename spans (part of [#453])
* fix: connect using DialPeer instead of DialAddress [#454]
* fix: compilation error when used as a dependency [#470]
* perf: use hash_hasher where the key is Cid [#467]
* chore: upgrade to libp2p 0.39.1, update most of the other deps with the notable exception of cid and multihash [#472]
* refactor(swarm): swarm cleanup following libp2p upgrade to v0.39.1 [#473]
* fix: strict ordering for DAG-CBOR-encoded map keys [#493]
* feat: upgrade libp2p to v0.43.0 [#499]
* feat(http): default values for --bits and --profile [#502]

[#429]: https://github.com/rs-ipfs/rust-ipfs/pull/429
[#428]: https://github.com/rs-ipfs/rust-ipfs/pull/428
[#423]: https://github.com/rs-ipfs/rust-ipfs/pull/423
[#421]: https://github.com/rs-ipfs/rust-ipfs/pull/421
[#439]: https://github.com/rs-ipfs/rust-ipfs/pull/439
[#442]: https://github.com/rs-ipfs/rust-ipfs/pull/442
[#444]: https://github.com/rs-ipfs/rust-ipfs/pull/444
[#446]: https://github.com/rs-ipfs/rust-ipfs/pull/446
[#453]: https://github.com/rs-ipfs/rust-ipfs/pull/453
[#454]: https://github.com/rs-ipfs/rust-ipfs/pull/454
[#470]: https://github.com/rs-ipfs/rust-ipfs/pull/470
[#467]: https://github.com/rs-ipfs/rust-ipfs/pull/467
[#472]: https://github.com/rs-ipfs/rust-ipfs/pull/472
[#473]: https://github.com/rs-ipfs/rust-ipfs/pull/473
[#493]: https://github.com/rs-ipfs/rust-ipfs/pull/493
[#499]: https://github.com/rs-ipfs/rust-ipfs/pull/499
[#502]: https://github.com/rs-ipfs/rust-ipfs/pull/502

# 0.2.1

* fix: restore_bootstrappers doesn't enable content discovery [#406]

[#406]: https://github.com/rs-ipfs/rust-ipfs/pull/406

# 0.2.0

First real release, with big changes and feature improvements. Started tracking
a changelog.
