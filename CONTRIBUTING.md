# Contributing to Rust IPFS

Welcome, and thank you for your interest in contributing to Rust IPFS. Issues and pull requests are encouraged. You can make a difference by tackling any of the following items:

* Implementing the remaining features and corresponding HTTP endpoints not yet covered. See https://areweipfsyet.rs for info on which those are.
* Additional tests and CI for existing functionality
* Examples, particularly those that showcase Rust's unique capaibilities or performance
* Any advancements toward `no_std` support
* Documentation
* Benchmarks

Also, several issues are labeled as [help wanted](https://github.com/rs-ipfs/rust-ipfs/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22+) and [good first issue](https://github.com/rs-ipfs/rust-ipfs/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22), so please review those as well.

## First Principles

Following these principles in your PRs will greatly increase your chances of a successful merge:

1. Keep the patch size minimal
2. Aim for high (but not absolute) code coverage in testing

By keeping the patch size minimal we hope to avoid difficult to review situations where there are lot of lines changed with only a few necessary changes. If you wish to submit a pull request for reorganizing something, please keep all unnecessary changes out.

For example, if you wanted to change the wording of this CONTRIBUTING.md file and dislike the fact that there is no static word wrap used, please push two separate pull requests to first change the wording, and finally to reformat the file.

## Target Build

Rust IPFS will always target the current _stable_ version of Rust that is released. Our CI/CD tests will reflect this. See [instructions here on how to install the rust toolchain](https://doc.rust-lang.org/book/ch01-01-installation.html).

### Compilation is slow, help!

We are still actively developing Rust IPFS, and as such compilation + linking times will vary and in many cases, worsen. This can be mitigated locally by installing the `lld` linker using your system's package manager.

For example, on Debian systems:

```bash
sudo apt install lld
```

Then in your `~/.cargo/config` file:

```toml
[build]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

Including this in the project root would cause the compiler to error out on systems that don't have `lld` installed, so we have not checked this into source yet.

## Contributing

We welcome all forms of contribution. Please open issues and PRs for:

- Reporting Bugs
- Suggesting Enhancements
- Adding new tests
- Adding new functionality
- Documentation-only updates

## Style

- Please separate stylistic things into separate PRs than functional changes
- Git Commit Messages should lean towards [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) but will not be enforced
- Rust code should conform to `rustfmt` and `clippy` before push, as the CI will catch errors there

## Security

If you have discovered a security vulnerability, please open an issue. We'd like to handle things as transparently as possible. If you don't feel like this is prudent, please visit us on one of the chat channels via the badges in the README. One of the core contributors will be able to talk to you about the disclosure.
