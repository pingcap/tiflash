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

source ${SRCPATH}/release-centos7-llvm/scripts/env.sh
INSTALL_DIR=${INSTALL_UT_DIR}

rm -rf /tests && ln -s ${SRCPATH}/tests /tests
rm -rf /tiflash && ln -s ${INSTALL_DIR} /tiflash

source /tests/docker/util.sh
export LLVM_PROFILE_FILE="/tiflash/profile/unit-test-%${NPROC}m.profraw"
show_env
ENV_VARS_PATH=/tests/docker/_env.sh OUTPUT_XML=${OUTPUT_XML} NPROC=${NPROC} /tests/run-gtest.sh
