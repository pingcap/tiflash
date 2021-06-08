#!/usr/bin/env bash
set -e
VERSION=20210324.2
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

wget https://github.com/abseil/abseil-cpp/archive/refs/tags/$VERSION.tar.gz
tar xzvf $VERSION.tar.gz
rm -rf $VERSION.tar.gz
cd abseil-cpp-$VERSION
cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=On
make -j$THREADS && make install
cd ..
rm -rf abseil-cpp-$VERSION