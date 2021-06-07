#!/usr/bin/env bash

set -e

VERSION="v1.38.0"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPTPATH}/util.sh
CMAKE_FLAGS_ADD=""

cd ~
git clone https://github.com/grpc/grpc.git -b ${VERSION}
cd grpc
git submodule update --init

cd ~/grpc
mkdir .build
cd .build
cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release ${CMAKE_FLAGS_ADD}
make -j ${THREADS}
make install


cd ~/grpc
rm -rf .build
mkdir .build
cd .build
cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release ${CMAKE_FLAGS_ADD}
make -j ${THREADS}
make install

protoc --version

rm -rf ~/grpc
