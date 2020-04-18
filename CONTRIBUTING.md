# Contributing to Rust IPFS

Welcome, and thank you for your interest in contributing to Rust IPFS. Issues and pull requests are encouraged. As we work through the efforts described in the approved IPFS Dev Grant, we're looking for help in the following

* Implementing endpoints and features not covered in the dev grant (See the [README](./README.md#roadmap))
* Tests and CI for existing functionality
* Examples
* Documentation

## First Principles

1. Keep the build time small
2. Aim for high (but not absolute) code coverage in testing

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

- Git Commit Messages should lean towards [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) but will not be enforced
- Rust code should conform to `rustfmt` and `clippy` before push, as the CI will catch errors there

## Security

If you have discovered a security vulnerability, please open an issue. We'd like to handle things as transparently as possible. If you don't feel like this is prudent, please visit us on one of the chat channels via the badges in the README. One of the core contributors will be able to talk to you about the disclosure.
