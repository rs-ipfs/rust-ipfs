<!-- Fill in the blank: -->

This PR ______, so that ____.

Additionally, updates should conform closely as possible to the [Definition of Done](https://github.com/ipfs/devgrants/tree/master/open-grants/ipfs-rust#definition-of-done) defined in the devgrant proposal. This includes:

- [ ] There is a working Rust implementation of the command’s functionality
- [ ] Code is “linted” i.e. code formatting via rustfmt and language idioms via clippy
- [ ] There is an HTTP binding for said command exposed via the IPFS daemon
    - [ ] (Optional) There is a CLI command that utilizes either the Rust APIs or the HTTP APIs
- [ ] There are functional and/or unit tests written, and they are passing
- [ ] There is suitable documentation. In our case, this means:
    - [ ] Each command has a usage example and API specification
    - [ ] Top-level commands with subcommands display usage instructions
    - [ ] Rustdoc tests are passing on all code-level comments
    - [ ] Differences between Rust’s implementation and Go or JS are explained
- [ ] There are passing conformance tests, approved by Protocol Labs
