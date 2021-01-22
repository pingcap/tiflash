#!/usr/bin/env bash

set -e

VERSION="v1.26.0"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPTPATH}/util.sh
is_arm=$(check_arm_arch)
CMAKE_FLAGS_ADD=""

cd ~
git clone https://github.com/grpc/grpc.git -b ${VERSION}
cd grpc
git submodule update --init

if [[ ${is_arm} == 1 ]]; then
    CMAKE_FLAGS_ADD="-DRUN_HAVE_STD_REGEX=0 -DRUN_HAVE_POSIX_REGEX=0 -DCOMPILE_HAVE_GNU_POSIX_REGEX=0"
    cd third_party/boringssl
    git apply ${SCRIPTPATH}/grpc-arm-compile.patch
fi

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
