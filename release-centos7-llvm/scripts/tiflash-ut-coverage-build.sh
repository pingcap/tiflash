#!/bin/bash

set -ueox pipefail
BASE_NAME=$(basename $0)
scriptpath="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $scriptpath/../..
  pwd -P
)}

CMAKE_BUILD_TYPE=Debug
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

source ${SRCPATH}/release-centos7-llvm/scripts/env.sh

BUILD_DIR=${BUILD_UT_DIR}
INSTALL_DIR=${INSTALL_UT_DIR}

rm -rf ${INSTALL_DIR}
mkdir -p ${INSTALL_DIR}

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake "${SRCPATH}" \
  -DENABLE_TESTS=ON \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DUSE_CCACHE=ON \
  -DTEST_LLVM_COVERAGE=ON \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON \
  -DRUN_HAVE_STD_REGEX=0 \
  -DCMAKE_PREFIX_PATH="/usr/local" \
  -GNinja

ninja gtests_dbms gtests_libcommon gtests_libdaemon
mv "${BUILD_DIR}/dbms/gtests_dbms" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libcommon/src/tests/gtests_libcommon" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libdaemon/src/tests/gtests_libdaemon" "${INSTALL_DIR}/"

ccache -s

ls -lh "${INSTALL_DIR}"
