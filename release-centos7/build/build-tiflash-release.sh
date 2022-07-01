#!/bin/bash

set -x

ARCH=$(uname -p)

command -v cmake-3.22 >/dev/null 2>&1
if [[ $? != 0 ]]; then
  curl -o /root/cmake-3.22.tar.gz http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/cmake-3.22.3-linux-${ARCH}.tar.gz
  cd /root
  tar zxf cmake-3.22.tar.gz
  rm -rf /usr/bin/cmake-3.22
  ln -s /root/cmake-3.22.3-linux-${ARCH}/bin/cmake /usr/bin/cmake-3.22
  rm cmake-3.22.tar.gz
fi
cmake-3.22 --version

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

if [ -d "$SRCPATH/contrib/kvproto" ]; then
  cd "$SRCPATH/contrib/kvproto"
  rm -rf cpp/kvproto
  ./scripts/generate_cpp.sh
  cd -
fi

if [ -d "$SRCPATH/contrib/tipb" ]; then
  cd "$SRCPATH/contrib/tipb"
  rm -rf cpp/tipb
  ./generate-cpp.sh
  cd -
fi

rm -rf ${SRCPATH}/libs/libtiflash-proxy
mkdir -p ${SRCPATH}/libs/libtiflash-proxy
ln -s ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so ${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so

BUILD_DIR="${SRCPATH}/release-centos7/build-release"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake-3.22 -S "${SRCPATH}" ${DEFINE_CMAKE_PREFIX_PATH} \
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

cd "${INSTALL_DIR}"
chrpath -d libtiflash_proxy.so "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"
ls -lh "${INSTALL_DIR}"
