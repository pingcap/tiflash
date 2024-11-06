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

CMAKE_BUILD_TYPE=$1
CMAKE_PREFIX_PATH=$2

if [[ -z ${CMAKE_BUILD_TYPE} ]]; then
  CMAKE_BUILD_TYPE="RELWITHDEBINFO"
fi

if [ $CMAKE_BUILD_TYPE == "TSAN" ] || [ $CMAKE_BUILD_TYPE == "ASAN" ]; then
  ENABLE_TESTS=ON
else
  ENABLE_TESTS=OFF
fi

DEFAULT_CMAKE_PREFIX_PATH="/usr/local/"
DEFINE_CMAKE_PREFIX_PATH="-DCMAKE_PREFIX_PATH=${DEFAULT_CMAKE_PREFIX_PATH}"

# https://cmake.org/cmake/help/latest/variable/CMAKE_PREFIX_PATH.html
# CMAKE_PREFIX_PATH should be semicolon-separated list
if [[ ${CMAKE_PREFIX_PATH} ]]; then
  DEFINE_CMAKE_PREFIX_PATH="-DCMAKE_PREFIX_PATH=\"${DEFAULT_CMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH}\""
  echo "CMAKE_PREFIX_PATH is \"${DEFAULT_CMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH}\""
fi

set -ueox pipefail

# workaround git ownership check
git config --global --add safe.directory "/build/tics"

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=$(
  cd ${SCRIPTPATH}/../..
  pwd -P
)
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
ENABLE_THINLTO=${ENABLE_THINLTO:-ON}
ENABLE_PCH=${ENABLE_PCH:-ON}

INSTALL_DIR="${SRCPATH}/release-rocky8-llvm/tiflash"
rm -rf ${INSTALL_DIR} && mkdir -p ${INSTALL_DIR}

if [ $CMAKE_BUILD_TYPE == "RELWITHDEBINFO" ]; then
  BUILD_DIR="$SRCPATH/release-rocky8-llvm/build-release"
  ENABLE_FAILPOINTS="OFF"
  JEMALLOC_NARENAS="-1"
else
  BUILD_DIR="$SRCPATH/release-rocky8-llvm/build-debug"
  ENABLE_FAILPOINTS="ON"
  JEMALLOC_NARENAS="40"
fi
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake -S "${SRCPATH}" \
  ${DEFINE_CMAKE_PREFIX_PATH} \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DENABLE_TESTING=OFF \
  -DENABLE_TESTS=${ENABLE_TESTS} \
  -DENABLE_FAILPOINTS=${ENABLE_FAILPOINTS} \
  -DJEMALLOC_NARENAS=${JEMALLOC_NARENAS} \
  -Wno-dev \
  -DUSE_CCACHE=OFF \
  -DUSE_INTERNAL_SSL_LIBRARY=ON \
  -DRUN_HAVE_STD_REGEX=0 \
  -DENABLE_THINLTO=${ENABLE_THINLTO} \
  -DTHINLTO_JOBS=${NPROC} \
  -DENABLE_PCH=${ENABLE_PCH} \
  -GNinja

if [ $CMAKE_BUILD_TYPE == "TSAN" ] || [ $CMAKE_BUILD_TYPE == "ASAN" ]; then
  cmake --build . --target gtests_dbms gtests_libcommon --parallel ${NPROC}
  cmake --build . --target tiflash --parallel ${NPROC}
  cmake --install . --component=tiflash-release --prefix="${INSTALL_DIR}"
else
  cmake --build . --target tiflash --parallel ${NPROC}
  cmake --install . --component=tiflash-release --prefix="${INSTALL_DIR}"
  
  # unset LD_LIBRARY_PATH before test
  unset LD_LIBRARY_PATH
  readelf -d "${INSTALL_DIR}/tiflash"
  ldd "${INSTALL_DIR}/tiflash"
  
  # show version
  ${INSTALL_DIR}/tiflash version
fi
