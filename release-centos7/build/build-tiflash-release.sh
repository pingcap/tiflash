#!/bin/bash

CMAKE_BUILD_TYPE=$1
CMAKE_PREFIX_PATH=$2

if [[ -z ${CMAKE_BUILD_TYPE} ]]; then
    CMAKE_BUILD_TYPE="RELWITHDEBINFO"
fi

DEFINE_CMAKE_PREFIX_PATH=""
if [[ ${CMAKE_PREFIX_PATH} ]]; then
    DEFINE_CMAKE_PREFIX_PATH="-DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}"
    echo "CMAKE_PREFIX_PATH is ${CMAKE_PREFIX_PATH}"
fi

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
ENABLE_EMBEDDED_COMPILER="FALSE"

INSTALL_DIR="${SRCPATH}/release-centos7/tiflash"

rm -rf ${SRCPATH}/libs/libtiflash-proxy
mkdir -p ${SRCPATH}/libs/libtiflash-proxy
ln -s ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so ${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so

BUILD_DIR="${SRCPATH}/release-centos7/build-release"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake "${SRCPATH}" ${DEFINE_CMAKE_PREFIX_PATH} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DENABLE_EMBEDDED_COMPILER=${ENABLE_EMBEDDED_COMPILER} \
      -DENABLE_ICU=OFF \
      -DENABLE_MYSQL=OFF \
      -DENABLE_TESTING=OFF \
      -DENABLE_TESTS=OFF \
      -Wno-dev \
      -DUSE_CCACHE=OFF

make -j ${NPROC} tiflash

# Reduce binary size by compressing.
objcopy --compress-debug-sections=zlib-gnu "${BUILD_DIR}/dbms/src/Server/tiflash"

cp -f "${BUILD_DIR}/dbms/src/Server/tiflash" "${INSTALL_DIR}/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "${INSTALL_DIR}/libtiflash_proxy.so"

ldd "${INSTALL_DIR}/tiflash"

ldd "${INSTALL_DIR}/tiflash" | grep 'libnsl.so' | grep '=>' | awk '{print $3}' | xargs -I {} cp {} "${INSTALL_DIR}"

cd "${INSTALL_DIR}"
chrpath -d libtiflash_proxy.so "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"
ls -lh "${INSTALL_DIR}"
