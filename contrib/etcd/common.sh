#!/usr/bin/env bash

function push() {
    pushd $1 >/dev/null 2>&1
}

function pop() {
    popd >/dev/null 2>&1
}

function sed_inplace()
{
	# bsd sed does not support --version.
	if `sed --version > /dev/null 2>&1`; then
		sed -i "$@"
	else
		sed -i '' "$@"
	fi
}

function clean_gogo_proto()
{
	local file=$1
	sed_inplace '/gogo.proto/d' ${file}
	sed_inplace '/option\ *(gogoproto/d' ${file}
	sed_inplace -e 's/\[.*gogoproto.*\]//g' ${file}
}
export -f clean_gogo_proto
