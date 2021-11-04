#!/usr/bin/env bash

set -x

SRC_TESTS_PATH="$(
	cd "$(dirname "$0")"
	pwd -P
)"

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
OUTPUT_XML=${OUTPUT_XML:-false}
ENV_VARS_PATH=${ENV_VARS_PATH:-./_env.sh}
# Parallel between test cases is not supported
SERIALIZE_TEST_CASES=${SERIALIZE_TEST_CASES:-true}
RUN_TESTS_PARALLEL=${RUN_TESTS_PARALLEL:-true}
CONTINUE_ON_ERROR="${1:-1}" # default 1

function run_test() {
	local name="$1"
	local bin_path=$(find . -name "$name")
	local args=""
	if [[ "$CONTINUE_ON_ERROR" -eq 1 ]]; then
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

function run_test_parallel() {
	test_bins="$1"
	local args=""
	if [[ ${SERIALIZE_TEST_CASES} == "true" ]]; then args="${args} --serialize_test_cases"; fi
	if [[ "${CONTINUE_ON_ERROR}" == "1" ]]; then
		args="${args} --gtest_catch_exceptions=1"
	else
		args="--gtest_break_on_failure --gtest_catch_exceptions=0"
	fi

	python ${SRC_TESTS_PATH}/gtest_parallel.py ${test_bins} --workers=${NPROC} ${args}
}

set -e

cd "${build_dir}"

# Set env variable to run test cases with test data
export ALSO_RUN_WITH_TEST_DATA=1

if [[ ${RUN_TESTS_PARALLEL} != "true" ]]; then
	tests=(
		"gtests_dbms"
		"gtests_libcommon"
		"gtests_libdaemon"
	)
	for test in ${tests[@]}; do
		run_test "$test"
	done
else
	run_test_parallel "${build_dir}/gtests_dbms ${build_dir}/gtests_libcommon ${build_dir}/gtests_libdaemon"
fi
