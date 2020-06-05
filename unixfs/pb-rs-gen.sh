#!/usr/bin/env bash

# don't really use this as the files do have local modifications:
# - removal of the broken owned container
# - fixing of clippy warnings
#   - added lifetime
#   - fix the lint ignore
#
set -eu

gen() {
	# strip the packages as apparently there is a bug with single-mod 0.8.2
	local tmpfile="$(mktemp).proto"
	grep -v '^package ' "$1" > "$tmpfile";
	retval=0
	local filename="$(basename "$1")"
	local output="$(dirname "$1")/${filename%.*}.rs"
	pb-rs --single-mod --output "$output" "$tmpfile" || retval=$?
	rm "$tmpfile"

	if [[ "$retval" -ne 0 ]]; then
		return $retval;
	fi

	# strip out the empty lines and fix the automatically generated comment
	# empty lines seem to be a problem for cargo fmt
	sed -i -Ee '/^\s*$/d' -e "s/'$(basename "$tmpfile")'/'$filename'/g" "$output"
	cargo fmt -- "$output"
}

gen src/pb/merkledag.proto src/pb
gen src/pb/unixfs.proto src/pb
