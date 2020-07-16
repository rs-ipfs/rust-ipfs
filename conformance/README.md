# ipfs-rust-conformance
> Conformance testing for Rust IPFS

This repository contains the scripts used to run interface conformance testing for Rust IPFS. It uses `js-ipfsd-ctl`, `npm-rust-ipfs-dep`, and `interface-js-ipfs-core`. This code will likely either be integrated into `ipfs-rust/rust-ipfs` directly or integrated via CI.

# Install

To clone the repository and ready it for use, first clone this repo:

```bash
$ git clone https://github.com/rs-ipfs/ipfs-rust-conformance
```

After cloning, use `setup.sh` to do `npm install` and patch any of the dependencies:

```bash
$ cd ipfs-rust-conformance
$ ./setup.sh
```

# Usage

```bash
$ npm test
```

For development, you can symlink http to your rust-ipfs-http executable, and
then use the `rust.sh` as IPFS_RUST_EXEC to run tests and capture all output
from the binary.

```bash
$ ln -s /path/to/rust-ipfs/target/ipfs-http ./http
$ IPFS_RUST_EXEC=$(pwd)/rust.sh npm test
$ cat /tmp/rust.log
```

# Patch management

We currently staying behind at `interface-ipfs-core@0.134.3` and the fixes we have upstreamed are also kept under `patches/`.

To create a new patch:

1. Fork https://github.com/ipfs/js-ipfs/
2. Clone locally
3. Checkout a new branch based on the tag for the `interface-ipfs-core` version
   we are currently depending on (see package.json)
4. Apply all of our patches from `patches/` with `git apply $ipfs_rust_conformance/patches/*`
5. Fix any new tests
6. When done, cherry pick your commits to a new branch based off the latest
   main js-ipfs branch
7. Submit PR
8. Squash your work, refer to your PR in the message
9. Remove existing `patches/`
9. Recreate the `patches/` with `git format-patch -o
   $ipfs-rust-conformance/patches $the_tag_you_based_your_work`

Where above:

 - `$ipfs_rust_conformance` points to your checkout of this repository
 - `$the_tag_you_based_your_work` is the tag you started working on js-ipfs
   working copy

Previously this has been done by first working on a patch and then backporting
it, which might be easier. Luckily the tests are low traffic. If you need help
wrestling with git, don't hesitate to ask for guidance; this is simpler than
it's step by step explanation looks like.

# Troubleshooting hangs

If the tests hang because of some deadlock on the `ipfs-http` side the tests
will print the final summary from `mocha` and then seem to wait forever. This
is true at least for the hangs seen so far, but you could run into a different
issues. Note that in addition to test timeouts the http client also has a
timeout, which can keep the `npm test` alive for less than ten seconds after
the summary has been printed.

What has worked previously is:

 1. disable tests until you find the one which causes the hang
 2. rerun tests using the `rust.sh` wrapper which gives you logs at `/tmp/rust.log`
 3. continue debugging over at `ipfs-http`

"Disable tests" as in, comment tests out or run them with `IPFS_RUST_EXEC=...
npm test -- --grep 'should XYZ'`. You should get to a single running test which
hangs. If the test does a lot, you can refactor that into a smaller one which
will only cause the hang and nothing else.

The use of `rust.sh` wrapper is critical as it'll give you the logging output.
If the test has multiple running instances you might be better of separating
logs into files per invocation of `rust.sh` by appending `.$$` to the log files
name, which will expand to the process id of the shell running the script.

"Continue debugging" is a more tricky one. What has worked previously is:

 1. attaching a `gdb` to a running process in the case of livelocks
 2. adding debugging in the case of "everything stalled"

(I might be using the term wrong here) Livelocks happen when a task (running on
either tokio or async-std) never returns from [`std::future::Future::poll`] but
stays busy all of the time. You can spot these by seeing a core being
utilitized by `ipfs-http` constantly. These are easy to track down by:

 1. attach `gdb -p $process_id path/to/your/ipfs-http`
 2. find the interesting thread `info threads` or by looking at [threads stacktraces]

In the "everything stalled" case a [`std::future::Future::poll`] has completed
with `Poll::Pending` without waking up the task for a new poll. These are quite
easy mistakes to made. Good indications of such issues:

 * custom `poll` methods without the [`std::task::Context`] parameter: these
   methods will never be able to schedule wakeups
 * polling some nested "pollable" thing and returning `Poll::Pending` following
   nested returning `Poll::Ready(_)`
   * if the inner "pollable" didn't return `Poll::Pending` it means it had
     "more values to bubble up"
   * see this hastly written issue
     https://github.com/libp2p/rust-libp2p/issues/1516 and the linked commit(s)
 * custom [`std::future::Future`] which cannot return errors on drop like with
   the early SubscriptionFuture (see
   https://github.com/ipfs-rust/rust-ipfs/pull/130)

[`why-is-node-running`]: https://www.npmjs.com/package/why-is-node-running
[`std::future::Future::poll`]: https://doc.rust-lang.org/std/future/trait.Future.html#tymethod.poll
[threads stacktraces]: https://stackoverflow.com/questions/18391808/how-do-i-get-the-backtrace-for-all-the-threads-in-gdb

# Contributing

Issues and pull requests welcome.

# License

TBD

## Trademarks

The [Rust logo and wordmark](https://www.rust-lang.org/policies/media-guide) are trademarks owned and protected by the [Mozilla Foundation](https://mozilla.org). The Rust and Cargo logos (bitmap and vector) are owned by Mozilla and distributed under the terms of the [Creative Commons Attribution license (CC-BY)](https://creativecommons.org/licenses/by/4.0/).
