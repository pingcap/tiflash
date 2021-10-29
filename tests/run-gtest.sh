#!/usr/bin/env bash

set -x

SRC_TESTS_PATH="$(
	cd "$(dirname "$0")"
	pwd -P
)"

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
OUTPUT_XML=${OUTPUT_XML:-false}
ENV_VARS_PATH=${ENV_VARS_PATH:-./_env.sh}
SERIALIZE_TEST_CASES=${SERIALIZE_TEST_CASES:-false}
RUN_TESTS_PARALLEL=${RUN_TESTS_PARALLEL:-false}

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

function run_test_parallel() {
	test_bins="$1"
	local args=""
	if [[ ${SERIALIZE_TEST_CASES} == "true" ]]; then args="${args} --serialize_test_cases"; fi
	if [[ "${continue_on_error}" == "1" ]]; then
		args="${args} --gtest_catch_exceptions=1"
	else
		args="--gtest_break_on_failure --gtest_catch_exceptions=0"
	fi

	python ${SRC_TESTS_PATH}/gtest_parallel.py ${test_bins} --workers=${NPROC} ${args}
}

set -e

cd "${build_dir}"

# Set env variable to run some special test cases.
export ALSO_RUN_WITH_TEST_DATA=1

if [[ ${RUN_TESTS_PARALLEL} != "true" ]]; then
	tests=(
		"gtests_dbms"
		"gtests_libcommon"
		"gtests_libdaemon"
		#"gtests_tmt" # included in gtests_dbms
	)
	for test in ${tests[@]}; do
		run_test "$test"
	done
else
	run_test_parallel "${build_dir}/gtests_dbms ${build_dir}/gtests_libcommon ${build_dir}/gtests_libdaemon"
fi
