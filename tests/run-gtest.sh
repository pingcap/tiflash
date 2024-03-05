#!/usr/bin/env bash
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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

	# run with 45 min timeout
	python ${SRC_TESTS_PATH}/gtest_parallel.py ${test_bins} --workers=${NPROC} ${args} --print_test_times --timeout=2700
}

set -e

cd "${build_dir}"

# ThreadSanitizerFlags
# Reference: https://github.com/google/sanitizers/wiki/ThreadSanitizerFlags
if [ -z $TSAN_OPTIONS ]; then
	# Ignore false positive error, related issue
	# https://github.com/pingcap/tiflash/issues/6766
	export TSAN_OPTIONS="report_atomic_races=0"
else
	export TSAN_OPTIONS="report_atomic_races=0 ${TSAN_OPTIONS}"
fi

# Set env variable to run test cases with test data
export ALSO_RUN_WITH_TEST_DATA=1
export LD_LIBRARY_PATH=.
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
