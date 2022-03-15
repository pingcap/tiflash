#!/bin/bash
# Copyright 2022 PingCAP, Ltd.
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
