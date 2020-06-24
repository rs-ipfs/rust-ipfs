# Next

* Hide `ipfs_unixfs::file::reader` to hide non-exported type on pub fn [#203]

[#203]: https://github.com/rs-ipfs/rust-ipfs/pull/203

# 0.0.1

Initial release.

* `ipfs_unixfs::walk::Walker` for walking directories, files and symlinks
* `ipfs_unixfs::resolve` for resoving a single named link over directories
  (plain or HAMT sharded)
* `ipfs_unixfs::file::visit::FileVisit` lower level file visitor
