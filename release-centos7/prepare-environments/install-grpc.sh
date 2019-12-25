#!/usr/bin/env bash

set -e

VERSION="v1.24.3"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

cd ~
git clone https://github.com/grpc/grpc.git
cd grpc
git checkout ${VERSION}
git submodule update --init

cd ~/grpc
mkdir .build
cd .build
cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release
make install -j $THREADS


cd ~/grpc
rm -rf .build
mkdir .build
cd .build
cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release
make install -j $THREADS

protoc --version

rm -rf ~/grpc
