#!/usr/bin/env bash

set -eu
set -o pipefail

if ! [ -f "./package.json" ]; then
    echo "Please run $0 from the conformance subdirectory" >&2
    exit 1
fi

if [ -d "node_modules" ]; then
    echo "Directory exists already: node_modules/" >&2
    exit 1
fi

# production will skip the dev dependencies
npm install --production

if [ -d "patches" ]; then
    echo "Applying patches..."
    # as we want to apply the patches create in a js-ipfs checkout to our node_modules
    # we'll need to remove a few leading path segments to match
    # a/packages/interface-ipfs-core/src/refs.js to node_modules/interface-ipfs-core/src/refs.js
    for p in patches/*; do
        echo "Applying $(basename "$p")..." >&2
        patch -d node_modules/ -p1 < "$p"
    done
fi

if ! [ -f "../target/debug/ipfs-http" ]; then
    echo "Please build a debug version of Rust IPFS first via `cargo build --workspace` in the project root first." >&2
    exit 1
fi

ln -s ../target/debug/ipfs-http http
