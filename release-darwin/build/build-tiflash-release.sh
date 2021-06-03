#!/bin/bash

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
NPROC=${NPROC:-$(sysctl -n hw.physicalcpu || grep -c ^processor /proc/cpuinfo)}
CMAKE_BUILD_TYPE="RELWITHDEBINFO"
ENABLE_EMBEDDED_COMPILER="FALSE"

install_dir="$SRCPATH/release-darwin/tiflash"

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
install_name_tool -id @executable_path/libtiflash_proxy.dylib ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.dylib
ln -s ${SRCPATH}/contrib/tiflash-proxy/target/release/libtiflash_proxy.dylib ${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.dylib

build_dir="$SRCPATH/release-darwin/build-release"
rm -rf $build_dir && mkdir -p $build_dir && cd $build_dir

cmake "$SRCPATH" \
      -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
      -DENABLE_ICU=OFF \
      -DENABLE_MYSQL=OFF \
      -Wno-dev \
      -DNO_WERROR=ON

make -j $NPROC tiflash

cp -f "$build_dir/dbms/src/Server/tiflash" "$install_dir/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.dylib" "$install_dir/libtiflash_proxy.dylib"

FILE="$install_dir/tiflash"
otool -L "$FILE"

set +e
echo "show ccache stats"
ccache -s