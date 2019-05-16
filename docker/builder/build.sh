#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

ENABLE_TEST=${ENABLE_TEST:-0}
ENABLE_EMBEDDED_COMPILER=${DENABLE_EMBEDDED_COMPILER:-1}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}

set -xe

if [ -d "$SRCPATH/contrib/kvproto" ]; then
  cd "$SRCPATH/contrib/kvproto"
  rm -rf cpp/kvproto
  ./generate_cpp.sh
  cd -
fi

build_dir="$SRCPATH/build_docker"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER -DENABLE_TESTS=$ENABLE_TEST -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE
make -j $NPROC
#ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)

install_dir="$SRCPATH/docker/builder/tics"
if [ -d "$install_dir" ]; then rm -rf "$install_dir"/*; else mkdir -p "$install_dir"; fi
cp -f "$build_dir/dbms/src/Server/theflash" "$install_dir/theflash"

ldd "$build_dir/dbms/src/Server/theflash" | grep '/' | grep '=>' | \
  awk -F '=>' '{print $2}' | awk '{print $1}' | while read lib; do
  cp -f "$lib" "$install_dir"
done
