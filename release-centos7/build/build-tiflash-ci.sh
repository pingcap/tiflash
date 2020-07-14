#!/bin/bash

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

ENABLE_TEST=${ENABLE_TEST:-1}
ENABLE_EMBEDDED_COMPILER="FALSE"
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}

if [[ "${CMAKE_BUILD_TYPE}" != "Debug" ]]; then
    ENABLE_TEST=0
fi

set -xe

install_dir="$SRCPATH/release-centos7/tiflash"
if [ -d "$install_dir" ]; then rm -rf "$install_dir"/*; else mkdir -p "$install_dir"; fi

cp -r /flash_cluster_manager "$install_dir"/flash_cluster_manager

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

# upload: curl -F builds/pingcap/tiflash-proxy/{commit-hash}/libtiflash_proxy.so=@libtiflash_proxy.so http://fileserver.pingcap.net/upload

curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" \
http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/8650732004b2d83375bd939cbf0ba51846e4c7c6/libtiflash_proxy.so
chmod 0731 "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so"

build_dir="$SRCPATH/release-centos7/build-release"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" \
    -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
    -DENABLE_TESTS=$ENABLE_TEST \
    -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE
make -j $NPROC

cp -f "$build_dir/dbms/src/Server/tiflash" "$install_dir/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "$install_dir/libtiflash_proxy.so"

# copy gtest binary under Debug mode
if [[ "${CMAKE_BUILD_TYPE}" = "Debug" && ${ENABLE_TEST} -ne 0 ]]; then
    #ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
    make -j ${NPROC} gtests_dbms gtests_libcommon
    cp -f "$build_dir/dbms/gtests_dbms" "$install_dir/"
    cp -f "$build_dir/libs/libcommon/src/tests/gtests_libcommon" "$install_dir/"
fi

ldd "$install_dir/tiflash"
cd "$install_dir"
chrpath -d libtiflash_proxy.so "$install_dir/tiflash"
ldd "$install_dir/tiflash"
