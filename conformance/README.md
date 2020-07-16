# Conformance testing for Rust IPFS

This directory contains the scripts used to run interface conformance testing
for Rust IPFS. It uses `js-ipfsd-ctl`, `ipfs-http-client`, and
`interface-js-ipfs-core`. This code used to live at
https://github.com/rs-ipfs/ipfs-rust-conformance, but was integrated into
https://github.com/rs-ipfs/rust-ipfs, as working with multiple repositories got
a bit tedious.

# Usage

Use `setup.sh` to do `npm install` and patch any of the dependencies:

```bash
$ cargo build -p ipfs-http
$ cd conformance
$ ./setup.sh
```

By default, there is a `http` symlink to `../target/debug/ipfs-http`. You can
change this to the release binary by modifying the symlink or use a custom
binary via the environment variable `IPFS_RUST_EXEC`. The default `rust.sh`
wrapper will record all actions taken by the tests into a single large log
file. It's not recommended to trust it to keep all log lines especially for
tests with multiple processes.

```bash
$ IPFS_RUST_EXEC="$(pwd)/rust.sh" npm test
$ cat /tmp/rust.log
```

## Obtaining logs for tests with multiple processes

Patch the `rust.sh` as follows:

```diff
-./http "$@" 2>&1 | tee -a /tmp/rust.log || retval=$?
+./http "$@" 2>&1 | tee -a /tmp/rust.log.$$ || retval=$?
```

Now the `/tmp/rust.log` will contain only the "pointers" to other log files, for example:

```
>>>> new execution 24132 with args: daemon
<<<< exiting 24132 with 0
```

This means there is now a log file `/tmp/rust.log.24132` for that invocation.

Additionally, it helps to clear out the logs often with `rm -f /tmp/rust.log*`
and only run selected tests using `IPFS_RUST_EXEC="$(pwd)/rust.sh" npm test -- --grep 'should do foo'`.
If it's impossible to limit the number of tests to one with `--grep`, you can
comment out the undesired tests in `test/index.js`.

# Patch management

We are currently pinned to `interface-ipfs-core@0.137.0` and the fixes we have upstreamed are kept under `patches/`.

To create a new patch:

1. Fork https://github.com/ipfs/js-ipfs
2. Clone locally
3. Checkout a new branch based on the tag for the `interface-ipfs-core` version
   we are currently depending on (see `package.json`)
4. Apply all of our patches from `patches/` with `git apply $ipfs_rust_conformance/patches/*`
5. Fix any new tests
6. When done, cherry-pick your commits to a new branch based off the latest
   main `js-ipfs` branch
7. Submit PR
8. Squash your work, refer to your PR in the message
9. Remove existing `patches/`
10. Recreate the `patches/` with `git format-patch -o
   $ipfs-rust-conformance/patches $the_tag_you_based_your_work`

`$variable` definitions:

 - `$ipfs_rust_conformance` points to your checkout of this repository
 - `$the_tag_you_based_your_work` is the tag where you started working on js-ipfs

Previously this has been done by first working on a patch and then backporting
it, which might be easier. Luckily the tests are low traffic. If you need help
wrestling with git, don't hesitate to ask for guidance; this is simpler than
its step by step explanation looks like.

# Troubleshooting hangs

If the tests hang because of some deadlock on the `ipfs-http` side the tests
will print the final summary from `mocha` and then seem to wait forever. This
is true at least for the hangs seen so far, but you could run into a different
issues. Note that in addition to test timeouts the http client also has a
timeout, which can keep the `npm test` alive for less than ten seconds after
the summary has been printed.

What has worked previously is:

 1. keep disabling some tests until you find the one which causes the hang
 2. rerun tests using the `rust.sh` wrapper which gives you logs at `/tmp/rust.log`
 3. continue debugging over at `ipfs-http`

To disable other test means to comment them or to use `IPFS_RUST_EXEC=...
npm test -- --grep '<suspected test>'`. You should get to a single running test which
hangs. If the test does a lot, you can refactor that into a smaller one which
will only cause the hang and nothing else.

The use of `rust.sh` wrapper is critical as it'll give you the logging output.
If the test has multiple running instances you might be better off separating
logs into files per invocation of `rust.sh` by appending `.$$` to the log files
name, which will expand to the process id of the shell running the script.

To "continue debugging" is trickier. What has worked previously is:

 1. attaching a `gdb` to a running process in case of livelocks
 2. adding debugging in case everything stalls

Livelocks (I might be using a wrong term here) happen when a task (running on
either tokio or async-std) never returns from [`std::future::Future::poll`] but
stays busy all of the time. You can spot these by seeing a core being
utilitized by `ipfs-http` constantly. These are easy to track down by:

 1. attach `gdb -p $process_id path/to/your/ipfs-http`
 2. find the interesting thread with `info threads` or by looking at [threads' stack traces]

In the "everything stalled" case, a [`std::future::Future::poll`] has completed
with `Poll::Pending` without waking up the task for a new poll. These mistakes are quite
simple to make. Good indications of such issues:

 * custom `poll` methods without the [`std::task::Context`] parameter: these
   methods will never be able to schedule wake-ups
 * polling some nested "pollable" thing and returning `Poll::Pending` following
   the nested poll returning `Poll::Ready(_)`
   * if the inner "pollable" didn't return `Poll::Pending`, it means it had
     "more values to bubble up"
   * see this hastly written issue
     https://github.com/libp2p/rust-libp2p/issues/1516 and the linked commit(s)
 * custom [`std::future::Future`] which cannot return errors on drop like with
   the early `SubscriptionFuture` (see
   https://github.com/ipfs-rust/rust-ipfs/pull/130)

[`why-is-node-running`]: https://www.npmjs.com/package/why-is-node-running
[`std::future::Future::poll`]: https://doc.rust-lang.org/std/future/trait.Future.html#tymethod.poll
[threads' stack traces]: https://stackoverflow.com/questions/18391808/how-do-i-get-the-backtrace-for-all-the-threads-in-gdb

# License

Same as the repository.

## Trademarks

The [Rust logo and wordmark](https://www.rust-lang.org/policies/media-guide) are trademarks owned and protected by the [Mozilla Foundation](https://mozilla.org). The Rust and Cargo logos (bitmap and vector) are owned by Mozilla and distributed under the terms of the [Creative Commons Attribution license (CC-BY)](https://creativecommons.org/licenses/by/4.0/).
