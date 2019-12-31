#!/bin/bash

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
CMAKE_BUILD_TYPE="RELWITHDEBINFO"
ENABLE_EMBEDDED_COMPILER="FALSE"

install_dir="$SRCPATH/release-centos7/tiflash"

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
mkdir -p ${SRCPATH}/libs/libtiflash-proxy
ln -s ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.so ${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so

build_dir="$SRCPATH/release-centos7/build-release"
rm -rf $build_dir && mkdir -p $build_dir && cd $build_dir

cmake "$SRCPATH" \
      -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
      -DENABLE_ICU=OFF \
      -DENABLE_MYSQL=OFF \
      -Wno-dev

make -j $NPROC

cp -f "$build_dir/dbms/src/Server/theflash" "$install_dir/theflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "$install_dir/libtiflash_proxy.so"
