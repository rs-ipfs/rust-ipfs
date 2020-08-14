# 0.1.0

* Initial facilities for building File trees [#220]
* Hide `ipfs_unixfs::file::reader` to hide non-exported type on pub fn [#203]
* More ergonomic `ipfs_unixfs::walk::Walker` [#269], [#272], [#294]
* Update `filetime` dependency [#278]
* Directory tree building [#284]
* Symlink block serialization [#306]

[#203]: https://github.com/rs-ipfs/rust-ipfs/pull/203
[#220]: https://github.com/rs-ipfs/rust-ipfs/pull/220
[#269]: https://github.com/rs-ipfs/rust-ipfs/pull/269
[#272]: https://github.com/rs-ipfs/rust-ipfs/pull/272
[#278]: https://github.com/rs-ipfs/rust-ipfs/pull/278
[#284]: https://github.com/rs-ipfs/rust-ipfs/pull/284
[#294]: https://github.com/rs-ipfs/rust-ipfs/pull/294
[#306]: https://github.com/rs-ipfs/rust-ipfs/pull/306

# 0.0.1

Initial release.

* `ipfs_unixfs::walk::Walker` for walking directories, files and symlinks
* `ipfs_unixfs::resolve` for resoving a single named link over directories
  (plain or HAMT sharded)
* `ipfs_unixfs::file::visit::FileVisit` lower level file visitor
