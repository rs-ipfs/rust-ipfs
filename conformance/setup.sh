#!/usr/bin/env bash

set -eu
set -o pipefail

if ! [ -f "./package.json" ]; then
    echo "Please run $0 from the conformance subdirectory" >&2
    exit 1
fi

# production will skip the dev dependencies
npm install --production

if [ -d "patches" ]; then
    echo "Applying patches..."
    # as we want to apply the patches create in a js-ipfs checkout to our node_modules
    # we'll need to remove a few leading path segments to match
    # a/packages/interface-ipfs-core/src/refs.js to node_modules/interface-ipfs-core/src/refs.js
    #
    # cannot use git automation any longer as it will skip any ignored file apparently,
    # and node_modules are ignored.
    for p in patches/*; do
        echo "Applying $(basename "$p")..." >&2
        patch -d node_modules/interface-ipfs-core/ -p3 < "$p"
    done
fi
