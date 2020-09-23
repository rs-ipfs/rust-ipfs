# ipfs-http crate

HTTP api on top of `ipfs` crate. The end binary has some rudimentary ipfs CLI
functionality but mostly in the aim of testing the `rust-ipfs` via:

 * [conformance](../conformance)
 * [interop](https://github.com/rs-ipfs/interop/)

The vision for this crate is to eventually provide warp filters and async
methods suitable to providing the Ipfs HTTP API in other applications as well
instead of having to write application specific debug and introspection APIs.

HTTP specs:

 * https://docs.ipfs.io/reference/http/api/

Status: Pre-alpha, most of the functionality is missing or `501 Not
Implemented`. See the repository level README for more information.
