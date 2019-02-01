#!/bin/sh

# This script regenerates the `bitswap_proto.rs` file from `bitswap.proto`.

docker run --rm -v `pwd`:/usr/code:z -w /usr/code rust /bin/bash -c " \
    apt-get update; \
    apt-get install -y protobuf-compiler; \
    cargo install --version 2.0.2 protobuf-codegen; \
    protoc --rust_out . bitswap.proto"

sudo chown $USER:$USER *.rs

mv -f bitswap.rs ./protobuf_structs/bitswap.rs
