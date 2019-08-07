#!/usr/bin/env bash

function run_test()
{
	local name="$1"
	local bin_path=$(find . -name "$name")
	${bin_path}
}

source ./_env.sh

set -ex

cd "$build_dir"

tests=(
	"gtests_dbms"
	"gtests_libcommon"
)

for test in ${tests[@]}; do
	run_test "$test"
done
