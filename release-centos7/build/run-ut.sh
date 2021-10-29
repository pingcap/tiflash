#!/bin/bash

scriptpath="$(
    cd "$(dirname "$0")"
    pwd -P
)"

SRCPATH=${1:-$(
    cd $scriptpath/../..
    pwd -P
)}

set -x

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
OUTPUT_XML=${OUTPUT_XML:-false}

INSTALL_DIR=${INSTALL_DIR:-"/build/release-centos7/tiflash"}

rm -rf /tests && ln -s ${SRCPATH}/tests /tests
rm -rf /tiflash && ln -s ${INSTALL_DIR} /tiflash

source /tests/docker/util.sh

show_env

ENV_VARS_PATH=/tests/docker/_env.sh OUTPUT_XML=${OUTPUT_XML} NPROC=${NPROC} RUN_TESTS_PARALLEL=true SERIALIZE_TEST_CASES=false /tests/run-gtest.sh
