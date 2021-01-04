# Next

* Document panic introduced in walker ergonomics [#435]

[#435]: https://github.com/rs-ipfs/rust-ipfs/pull/435

# 0.2.0

Minor version bump due to ipfs 0.2.0 release.

* Clippy future-proofing [#316]
* Use core and alloc crates instead of std [#331]
* `crate::dagpb::wrap_node_data`, part of [#332]
* minor doc fix for `crate::dir::ResolveError::UnexpectedType`, part of [#332]

[#316]: https://github.com/rs-ipfs/rust-ipfs/pull/316
[#331]: https://github.com/rs-ipfs/rust-ipfs/pull/331
[#332]: https://github.com/rs-ipfs/rust-ipfs/pull/332

# 0.1.0

Minor version bump due to many new features and broken API.

* Initial facilities for building File trees [#220]
* Hide `ipfs_unixfs::file::reader` to hide non-exported type on pub fn [#203]
* More ergonomic `ipfs_unixfs::walk::Walker` [#269], [#272], [#294]
* Update `filetime` dependency [#278]
* Directory tree building [#284]
* Symlink block serialization [#302]

[#203]: https://github.com/rs-ipfs/rust-ipfs/pull/203
[#220]: https://github.com/rs-ipfs/rust-ipfs/pull/220
[#269]: https://github.com/rs-ipfs/rust-ipfs/pull/269
[#272]: https://github.com/rs-ipfs/rust-ipfs/pull/272
[#278]: https://github.com/rs-ipfs/rust-ipfs/pull/278
[#284]: https://github.com/rs-ipfs/rust-ipfs/pull/284
[#294]: https://github.com/rs-ipfs/rust-ipfs/pull/294
[#302]: https://github.com/rs-ipfs/rust-ipfs/pull/302

# 0.0.1

Initial release.

* `ipfs_unixfs::walk::Walker` for walking directories, files and symlinks
* `ipfs_unixfs::resolve` for resoving a single named link over directories
  (plain or HAMT sharded)
* `ipfs_unixfs::file::visit::FileVisit` lower level file visitor
