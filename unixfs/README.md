# ipfs-unixfs

Goals:

* blockstore API independent way to traverse the merkledag
    * the core read API does not deal with loading blocks
    * instead access to interesting `Cid`s is given

Status:

* first iteration of file reader has been implemented
* directory support has not been implemented
* creation and alteration of dags has not been implemented
