#!/usr/bin/env bash

set -e

VERSION="v1.38.0"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPTPATH}/util.sh
CMAKE_FLAGS_ADD=""

cd ~
git clone https://github.com/grpc/grpc.git -b ${VERSION} --recursive --depth=1 --progress
cd grpc

cd ~/grpc
mkdir .build
cd .build
cmake .. -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl \
  -DgRPC_BUILD_TESTS=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DgRPC_SSL_PROVIDER=package \
  -DgRPC_ABSL_PROVIDER=package
make -j ${THREADS}
make install


cd ~/grpc
rm -rf .build
mkdir .build
cd .build
cmake .. \
  -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl \
  -DgRPC_RE2_PROVIDER=package \
  -DgRPC_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF \
  -DgRPC_ABSL_PROVIDER=package \
  -DgRPC_PROTOBUF_PROVIDER=package \
  -DgRPC_ZLIB_PROVIDER=package \
  -DgRPC_CARES_PROVIDER=package \
  -DgRPC_SSL_PROVIDER=package \
  -DCMAKE_BUILD_TYPE=Release ${CMAKE_FLAGS_ADD}
make -j ${THREADS}
make install

protoc --version

rm -rf ~/grpc
