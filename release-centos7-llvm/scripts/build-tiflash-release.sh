#!/usr/bin/env bash

CMAKE_BUILD_TYPE=$1
CMAKE_PREFIX_PATH=$2

if [[ -z ${CMAKE_BUILD_TYPE} ]]; then
    CMAKE_BUILD_TYPE="RELWITHDEBINFO"
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

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)
ENABLE_EMBEDDED_COMPILER="FALSE"

INSTALL_DIR="${SRCPATH}/release-centos7-llvm/tiflash"
BUILD_DIR="${SRCPATH}/release-centos7-llvm/build-release"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake "${SRCPATH}" ${DEFINE_CMAKE_PREFIX_PATH} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DENABLE_EMBEDDED_COMPILER=${ENABLE_EMBEDDED_COMPILER} \
      -DENABLE_TESTING=OFF \
      -DENABLE_TESTS=OFF \
      -Wno-dev \
      -DUSE_CCACHE=OFF \
      -DLINKER_NAME=lld \
      -DUSE_LIBCXX=ON \
      -DUSE_LLVM_LIBUNWIND=OFF \
      -DUSE_LLVM_COMPILER_RT=OFF \
      -DTIFLASH_ENABLE_RUNTIME_RPATH=ON \
      -DRUN_HAVE_STD_REGEX=0 \
      -DCMAKE_AR="/usr/local/bin/llvm-ar" \
      -DCMAKE_RANLIB="/usr/local/bin/llvm-ranlib" \
      -GNinja

ninja tiflash

source ${SCRIPTPATH}/utils/vendor_dependency.sh

# compress debug symbols
llvm-objcopy --compress-debug-sections=zlib-gnu "${BUILD_DIR}/dbms/src/Server/tiflash" "${INSTALL_DIR}/tiflash"

vendor_dependency "${INSTALL_DIR}/tiflash" libc++.so    "${INSTALL_DIR}/"
vendor_dependency "${INSTALL_DIR}/tiflash" libc++abi.so    "${INSTALL_DIR}/"

cp -f "${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so" "${INSTALL_DIR}/libtiflash_proxy.so"

# unset LD_LIBRARY_PATH before test
unset LD_LIBRARY_PATH
readelf -d "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"

