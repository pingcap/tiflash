#!/bin/bash

set -xe

mkdir -p /server/build_docker
cd /server/contrib/kvproto
rm -rf cpp/kvproto
./generate_cpp.sh
cd /server/build_docker
cmake /server -DENABLE_EMBEDDED_COMPILER=1 -DENABLE_TESTS=1 -DCMAKE_BUILD_TYPE=Debug
make -j $(nproc || grep -c ^processor /proc/cpuinfo)
#ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)

install_dir="/server/docker/builder/tics"
mkdir -p "$install_dir"
rm -rf $install_dir/*
cp -f "/server/build_docker/dbms/src/Server/theflash" "$install_dir"

ldd "/server/build_docker/dbms/src/Server/theflash" | grep '/' | grep '=>' | \
  awk -F '=>' '{print $2}' | awk '{print $1}' | while read lib; do
  cp -f "$lib" "$install_dir"
done
