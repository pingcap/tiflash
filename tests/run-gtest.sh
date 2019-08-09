#!/usr/bin/env bash

function run_test()
{
	local name="$1"
	local bin_path=$(find . -name "$name")
    if [[ "$continue_on_error" -eq 1 ]]; then
        ${bin_path} --gtest_catch_exceptions=1
    else
        ${bin_path} --gtest_break_on_failure --gtest_catch_exceptions=0
    fi
}

source ./_env.sh

continue_on_error="${1:-1}" # default 1
set -ex

cd "$build_dir"

tests=(
	"gtests_dbms"
	"gtests_libcommon"
)

for test in ${tests[@]}; do
	run_test "$test"
done
