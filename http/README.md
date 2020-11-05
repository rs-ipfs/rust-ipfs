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

## Getting started

This tutorial will demonstrate how to run a rust-ipfs node using the ipfs-http
crate. If you haven't already, you'll need to [install
Rust](https://doc.rust-lang.org/stable/book/ch01-01-installation.html). You
should also [install the go-ipfs
CLI](https://docs.ipfs.io/install/command-line/) as this will make it easier to
interact with the node. 

By default ipfs-http stores the configuration for the node in the `.rust-ipfs`
directory. Should you want to override this, you can do so by setting the
`IPFS_PATH` environment variable. For this example it's a good idea to set the
path to `.rust-ipfs` so that the go-ipfs CLI knows to use that directory as
well (default is `.ipfs`). 

You can initialise the directory with: 

```
cargo run -p ipfs-http -- init --profile test --bits 2048
```

The `--profile` option allows the user to use a set of defaults. Currently two
profiles are supported:

- `test`: runs the daemon using ephemeral ports
- `default` runs the daemon on port `4004`

The `--bits` option specifies the length of the RSA keys to be used. The output
should return a peer id and confirm the path of the newly initialised node is
either the default or the `IPFS_PATH`.

The `.rust-ipfs` directory now contains a configuration file, `config`:

```
{
  "Identity": {
    "PeerID": "QmTETy4bmL44fwkvbkMzXMVmiUDTvEcupsfpM8BCgNERUe",
    "PrivKey": "CAASpgkwggSiAgEAAoIBAQCyFR6pKSRt62WLJ6fi2MeG0pn [...]" 
  },
  "Addresses": {
    "Swarm": [
      "/ip4/127.0.0.1/tcp/0"
    ]
  }
}
```

It stores the peer id, private key (shortened for brevity) and swarm addresses
for the node. Let's run the node as a daemon:

```
cargo run -p ipfs-http -- daemon
```

This exposes the node as an HTTP API. The config directory has also grown to
include a `blockstore`, a `datastore` and an `api` file:

```
.rust-ipfs
├── api
├── blockstore
├── config
└── datastore
    └── pins
```

The `blockstore` and `datastore` are empty, as we haven't yet added any data to
the ipfs node. The `api` file keeps track of the node's address.

The node can now be queried using the go-ipfs CLI. In another terminal window
run:

```
ipfs id
```

This returns the information about the node. 

```
{
	"ID": "QmTETy4bmL44fwkvbkMzXMVmiUDTvEcupsfpM8BCgNERUe",
	"PublicKey": "CAASpgIwggEiMA0GCSqGSIb3D [...]",
	"Addresses": [
		"/ip4/127.0.0.1/tcp/58807/p2p/QmTETy4bmL44fwkvbkMzXMVmiUDTvEcupsfpM8BCgNERUe"
	],
	"AgentVersion": "rust-ipfs/version",
	"ProtocolVersion": "ipfs/version",
	"Protocols": null
}
```

The query is logged by the node and shows the `/api/v0/id` endpoint handled the request:

```
INFO ipfs-http: 127.0.0.1:58811 "POST /api/v0/id HTTP/1.1" 200 "-" "go-ipfs-cmds/http" 2.795971ms
```


