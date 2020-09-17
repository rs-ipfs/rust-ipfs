const log = require('why-is-node-running')
const { createFactory } = require('ipfsd-ctl')
const tests = require('interface-ipfs-core')
const isDev = process.env.IPFS_RUST_EXEC

const isNode = (process && process.env)

const ipfsBin = isNode ?
  process.env.IPFS_RUST_EXEC ? process.env.IPFS_RUST_EXEC : require('rust-ipfs-dep').path()
    : undefined

after(() => {
  const whyIsNodeRunning = setTimeout(() => log(), 1000 * 60)
  // this should not block shutting down
  whyIsNodeRunning.unref()
})

const options = {
  type: 'rust',
  ipfsBin,
  test: true,
  disposable: true,
  ipfsHttpModule: require('ipfs-http-client'),
  ipfsOptions: {
    init: {
      bits: 2048
    }
  }
}

const factory = createFactory(options)

// Phase 1.0-ish
//
tests.miscellaneous(factory, { skip: [
  // the cidBase param is not implemented yet
  'should resolve an IPFS hash and return a base64url encoded CID in path',
  // different Cid, the /path/to/testfile.txt suffix shouldn't be there
  'should resolve an IPFS path link',
  // different Cid, missing "/path/to" in the middle
  'should resolve up to the last node across multiple nodes',
  // expected "true", got "false"
  'should resolve an IPNS DNS link',
  // HTTP: not implemented
  'should resolve IPNS link recursively',
  // these cause a hang 20% of time:
  'should respect timeout option when getting the node id',
  'should respect timeout option when getting the node version',
  // this hangs on windows, see #251
  'stop',
] })

// Phase 1.1

// these are a bit flaky
tests.pubsub(factory)
// these are rarely flaky
tests.swarm(factory)

// Phase 1.2

// ignored as the test doesn't pass at all through ipfs-http-client even
// against jsipfs. we do at least return the same value.
tests.dag.get(factory, { skip: ['should get only a CID, due to resolving locally only'] })
tests.dag.put(factory)

tests.block(factory, {
  skip: [
    // both are pinning related
    'should put a buffer, using options'
  ]
})

tests.bitswap(factory);
tests.root.refs(factory);
tests.root.refsLocal(factory);

// Phase 2 and beyond...

tests.root.cat(factory);
tests.root.get(factory);
tests.root.add(factory, {
  skip: [
    // ordered in the order of most likely implementation
    // unixfsv1.5 metadata
    "should add with mode as string",
    "should add with mode as number",
    "should add with mtime as Date",
    "should add with mtime as { nsecs, secs }",
    "should add with mtime as timespec",
    "should add with mtime as hrtime",
    // raw leaves
    "should respect raw leaves when file is smaller than one block and no metadata is present",
    "should override raw leaves when file is smaller than one block and metadata is present",
    // only-hash=true requires "external block store" or filestore
    "should add with only-hash=true",
    "should add a directory with only-hash=true",
    "should add a file from the file system with only-hash=true",
    // remote
    "should add from a HTTP URL",
    "should add from a HTTP URL with redirection",
    "should add from a URL with only-hash=true",
    "should add from a URL with wrap-with-directory=true",
    "should add from a URL with wrap-with-directory=true and URL-escaped file name",
    // this might hang on windows at least; seems that there is a DNSCHANNEL open
    // see https://github.com/rs-ipfs/rust-ipfs/pull/251/checks?check_run_id=889139927#step:17:934
    "should not add from an invalid url",
  ]
});

tests.dht(factory, {
  skip: [
    // the found address has a much higher port than the expected one
    "should find other peers",
    // no auto-bootstrapping?
    "should be able to find providers",
    // multiple entries in the param
    "should allow multiple CIDs to be passed",
    // unimplemented endpoints
    "should respect timeout option when putting a value into the DHT",
    "should put a value to the DHT",
    "should respect timeout option when getting a value from the DHT",
    "should get a value after it was put on another node",
  ]
});

// tests.repo(factory)
// tests.object(factory)
tests.pin.add(factory, {
  skip: [
    'should respect timeout option when pinning a block'
  ]
})
tests.pin.ls(factory, {
  skip: [
    'should respect timeout option when listing pins',
    // ignore these for now: https://github.com/rs-ipfs/rust-ipfs/issues/350
    'should throw an error on missing direct pins for existing path',
    'should throw an error on missing link for a specific path',
    'should list indirect pins for a specific path',
  ]
})
tests.pin.rm(factory, {
  skip: [
    'should respect timeout option when unpinning a block'
  ]
})

tests.bootstrap(factory);

// tests.name(factory)
// tests.namePubsub(factory)
// tests.ping(factory)
// tests.key(factory)
// tests.config(factory)
// tests.stats(factory)
// tests.files(factory)
