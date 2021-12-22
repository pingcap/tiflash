#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install gRPC for CI/CD.
# Require: git, cmake
# Env: CC, CXX, CFLAGS, CXXFLAGS

function install_grpc() {
    # $1: grpc_version
    
    git clone https://github.com/grpc/grpc.git -b v$1 --depth=1 --recursive
    mkdir -p grpc/build 
    cd grpc/build
    cmake .. -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DLINKER_NAME=lld \
        -GNinja \
        -DCMAKE_C_COMPILER=${CC} \
        -DCMAKE_CXX_COMPILER=${CXX} \
        -DCMAKE_C_FLAGS="${CFLAGS} -DPTRACE_O_EXITKILL=0x100000 -w" \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -DPTRACE_O_EXITKILL=0x100000 -w" 
    ninja
    ninja install

    cd .. 
    rm -rf build
    mkdir build
    cd build
    cmake .. -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DgRPC_INSTALL=ON \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_ZLIB_PROVIDER=package \
        -DgRPC_CARES_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package \
        -DCMAKE_C_COMPILER=${CC} \
        -DCMAKE_CXX_COMPILER=${CXX} \
        -DCMAKE_C_FLAGS="${CFLAGS} -DPTRACE_O_EXITKILL=0x100000 -w" \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -DPTRACE_O_EXITKILL=0x100000 -w" 
    
    ninja
    ninja install
    cd ../..
    rm -rf grpc
}