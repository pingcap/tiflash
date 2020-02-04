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

cp -r /flash_cluster_manager "$install_dir"

if [ -d "$SRCPATH/contrib/kvproto" ]; then
  cd "$SRCPATH/contrib/kvproto"
  rm -rf cpp/kvproto
  ./generate_cpp.sh
  cd -
fi

if [ -d "$SRCPATH/contrib/tipb" ]; then
  cd "$SRCPATH/contrib/tipb"
  rm -rf cpp/tipb
  ./generate-cpp.sh
  cd -
fi

rm -rf ${SRCPATH}/libs/libtiflash-proxy
ln -s /libtiflash-proxy ${SRCPATH}/libs/libtiflash-proxy

build_dir="$SRCPATH/release-centos7/build-release"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" \
    -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
    -DENABLE_TESTS=$ENABLE_TEST \
    -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE
make -j $NPROC

cp -f "$build_dir/dbms/src/Server/theflash" "$install_dir/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "$install_dir/libtiflash_proxy.so"

# copy gtest binary under Debug mode
if [[ "${CMAKE_BUILD_TYPE}" = "Debug" && ${ENABLE_TEST} -ne 0 ]]; then
    #ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
    make -j ${NPROC} gtests_dbms gtests_libcommon gtests_tmt
    cp -f "$build_dir/dbms/gtests_dbms" "$install_dir/"
    cp -f "$build_dir/libs/libcommon/src/tests/gtests_libcommon" "$install_dir/"
    cp -f "$build_dir/dbms/src/Storages/Transaction/tests/gtests_tmt" "$install_dir/"
fi
