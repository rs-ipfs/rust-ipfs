# ipfs-unixfs

Goals:

* blockstore API independent way to traverse the merkledag
    * the core read API does not deal with loading blocks
    * instead access to interesting `Cid`s is given

Status:

* first iteration of file reader has been implemented
* first iteration of resolving IpfsPath segments through directories has been
  implemented
    * as the HAMTShard structure is not fully understood, all buckets are
      searched, however the API is expected to remain same even if more
      efficient lookup is implemented
* first iteration of `/get` alike tree walking implemented
* creation and alteration of dags has not been implemented

Usage:

* The main entry point to walking anything unixfs should be `ipfs_unixfs::walk::Walker`
* The main entry point to resolving links under dag-pb or unixfs should be `ipfs_unixfs::resolve`
* There is a `ipfs_unixfs::file::visit::FileVisit` facility but it should be
  considered superceded by `ipfs_unixfs::walk::Walker`
