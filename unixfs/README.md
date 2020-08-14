# ipfs-unixfs

## Goals

* blockstore API independent way to traverse the merkledag
    * the core read API does not deal with loading blocks
    * instead access to interesting `Cid`s is given

## Status

* unfiltered walking of known unixfs trees
* creation of balanced file trees
* creation of non HAMT-sharded directory trees

See the docs at https://docs.rs/ipfs-unixfs.

## License

MIT or APL2.
