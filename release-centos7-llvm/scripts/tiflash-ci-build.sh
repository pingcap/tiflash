#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}

echo "INSTALL_DIR=${INSTALL_DIR}"

TIFLASH_PROXY_BIN_PATH=${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so
rm -f ${TIFLASH_PROXY_BIN_PATH}

if [[ -f /tmp/libtiflash_proxy.so ]]; then
  BUILD_TIFLASH_PROXY=false
  CMAKE_PREBUILT_LIBS_ROOT_ARG=-DPREBUILT_LIBS_ROOT="${SRCPATH}/contrib/tiflash-proxy"
  cp /tmp/libtiflash_proxy.so ${TIFLASH_PROXY_BIN_PATH}
else
  BUILD_TIFLASH_PROXY=true
  CMAKE_PREBUILT_LIBS_ROOT_ARG=""
  echo "need to build libtiflash_proxy.so"
  export PATH=$PATH:$HOME/.cargo/bin
fi

CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
BUILD_BRANCH=${BUILD_BRANCH:-master}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
ENABLE_TESTS=${ENABLE_TESTS:-1}
USE_CCACHE=${USE_CCACHE:-ON}

if [[ "${CMAKE_BUILD_TYPE}" != "Debug" ]]; then
  ENABLE_TESTS=0
fi

rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

BUILD_DIR="${SRCPATH}/release-centos7-llvm/build-release"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}
cmake "$SRCPATH" ${CMAKE_PREBUILT_LIBS_ROOT_ARG} \
  -DENABLE_TESTS=${ENABLE_TESTS} \
  -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
  -DUSE_CCACHE=${USE_CCACHE} \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON \
  -DLINKER_NAME=lld \
  -DUSE_LIBCXX=ON \
  -DUSE_LLVM_LIBUNWIND=OFF \
  -DUSE_LLVM_COMPILER_RT=OFF \
  -DTIFLASH_ENABLE_RUNTIME_RPATH=ON \
  -DCMAKE_PREFIX_PATH="/usr/local" \
  -DCMAKE_AR="/usr/local/bin/llvm-ar" \
  -DRUN_HAVE_STD_REGEX=0 \
  -DCMAKE_RANLIB="/usr/local/bin/llvm-ranlib" \
  -DUSE_INTERNAL_TIFLASH_PROXY=${BUILD_TIFLASH_PROXY} \
  -GNinja

ninja tiflash

if [[ "${CMAKE_BUILD_TYPE}" = "Debug" && ${ENABLE_TESTS} -ne 0 ]]; then
  ninja page_ctl
  ninja page_stress_testing
  # build gtest in `release-centos7/build/build-tiflash-ut-coverage.sh`
fi

ccache -s

source ${SCRIPTPATH}/utils/vendor_dependency.sh

# Reduce binary size by compressing.
llvm-objcopy --compress-debug-sections=zlib-gnu "${BUILD_DIR}/dbms/src/Server/tiflash" "${INSTALL_DIR}/tiflash"

vendor_dependency "${INSTALL_DIR}/tiflash" libc++.so "${INSTALL_DIR}/"
vendor_dependency "${INSTALL_DIR}/tiflash" libc++abi.so "${INSTALL_DIR}/"

cp -f "${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so" "${INSTALL_DIR}/libtiflash_proxy.so"

# unset LD_LIBRARY_PATH before test
unset LD_LIBRARY_PATH
readelf -d "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"
ls -lha "${INSTALL_DIR}"
