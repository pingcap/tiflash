#!/usr/bin/env bash

OUTPUT_XML=${OUTPUT_XML:-false}
ENV_VARS_PATH=${ENV_VARS_PATH:-./_env.sh}

function run_test() {
	local name="$1"
	local bin_path=$(find . -name "$name")
	local args=""
	if [[ "$continue_on_error" -eq 1 ]]; then
		args="--gtest_catch_exceptions=1"
	else
		args="--gtest_break_on_failure --gtest_catch_exceptions=0"
	fi
	if [[ "${OUTPUT_XML}" == "true" ]]; then
		args="${args} --gtest_output=xml"
	fi
	${bin_path} ${args}
}

source ${ENV_VARS_PATH}

continue_on_error="${1:-1}" # default 1
set -ex

cd "${build_dir}"

tests=(
	"gtests_dbms"
	"gtests_libcommon"
	"gtests_libdaemon"
	#"gtests_tmt" # included in gtests_dbms
)

# Set env variable to run some special test cases.
export ALSO_RUN_WITH_TEST_DATA=1

for test in ${tests[@]}; do
	run_test "$test"
done
