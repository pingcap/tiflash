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

install_dir="$SRCPATH/release-centos7/tiflash"

rm -rf ${SRCPATH}/libs/libtiflash-proxy
mkdir -p ${SRCPATH}/libs/libtiflash-proxy
ln -s ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so ${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so

build_dir="$SRCPATH/release-centos7/build-release"
rm -rf $build_dir && mkdir -p $build_dir && cd $build_dir

cmake "$SRCPATH" ${DEFINE_CMAKE_PREFIX_PATH} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DENABLE_EMBEDDED_COMPILER=${ENABLE_EMBEDDED_COMPILER} \
      -DENABLE_ICU=OFF \
      -DENABLE_MYSQL=OFF \
      -DENABLE_TESTING=OFF \
      -DENABLE_TESTS=OFF \
      -Wno-dev \
      -DUSE_CCACHE=OFF

make -j $NPROC tiflash

# Reduce binary size by compressing.
objcopy --compress-debug-sections=zlib-gnu "$build_dir/dbms/src/Server/tiflash"

cp -f "$build_dir/dbms/src/Server/tiflash" "$install_dir/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "$install_dir/libtiflash_proxy.so"

ldd "$install_dir/tiflash"
cd "$install_dir"
chrpath -d libtiflash_proxy.so "$install_dir/tiflash"
ldd "$install_dir/tiflash"
