#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

set -xe

if [ -d "$SRCPATH/contrib/kvproto" ]; then
  cd "$SRCPATH/contrib/kvproto"
  rm -rf cpp/kvproto
  ./generate_cpp.sh
  cd -
fi

build_dir="$SRCPATH/build_docker"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" -DENABLE_EMBEDDED_COMPILER=1 -DENABLE_TESTS=1 -DCMAKE_BUILD_TYPE=Debug
make -j $NPROC
#ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)

install_dir="$SRCPATH/docker/builder/tics"
if [ -d "$install_dir" ]; then mkdir -p "$install_dir"; else rm -rf "$install_dir"/*; fi
cp -f "$build_dir/dbms/src/Server/theflash" "$install_dir"

ldd "$build_dir/dbms/src/Server/theflash" | grep '/' | grep '=>' | \
  awk -F '=>' '{print $2}' | awk '{print $1}' | while read lib; do
  cp -f "$lib" "$install_dir"
done
