#!/usr/bin/env sh

protoc --rust_out src/bitswap src/bitswap/bitswap_pb.proto
protoc --rust_out src/ipld/formats/pb src/ipld/formats/pb/dag_pb.proto
