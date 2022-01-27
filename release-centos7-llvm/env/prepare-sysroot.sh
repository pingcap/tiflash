#!/usr/bin/env bash
# Copyright (C) PingCAP, Inc.

set -ueox pipefail

CMAKE_VERSION=3.22.1
GO_VERSION="1.17"
ARCH=$(uname -m)
GO_ARCH=$([[ "$ARCH" == "aarch64" ]] && echo "arm64" || echo "amd64")
LLVM_VERSION="13.0.0"
CURL_VERSION="7.80.0"
CCACHE_VERSION="4.5.1"
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SYSROOT="$SCRIPTPATH/sysroot"
OPENSSL_VERSION="1_1_1l"

function install_cmake() {
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-linux-$ARCH.sh
    sh cmake-$CMAKE_VERSION-linux-$ARCH.sh --prefix="$SYSROOT" --skip-license --exclude-subdir
    rm -rf cmake-$CMAKE_VERSION-linux-$ARCH.sh
}

function install_llvm() {
    git clone https://github.com/llvm/llvm-project --depth=1 -b llvmorg-$LLVM_VERSION

    mkdir -p llvm-project/build
    cd llvm-project/build

    cmake -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DLLVM_ENABLE_PROJECTS="clang;lld;polly;clang-tools-extra" \
        -DLLVM_ENABLE_RUNTIMES="compiler-rt;libcxx;libcxxabi;libunwind;openmp" \
        -DLLVM_TARGETS_TO_BUILD=Native \
        -DLIBCXX_USE_COMPILER_RT=ON \
        -DLIBCXXABI_USE_COMPILER_RT=ON \
        -DLIBCXXABI_USE_LLVM_UNWINDER=ON \
        -DLIBUNWIND_USE_COMPILER_RT=ON \
        -DCOMPILER_RT_USE_BUILTINS_LIBRARY=ON \
        -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
        -DCLANG_DEFAULT_LINKER=lld \
        -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
        -DCLANG_DEFAULT_RTLIB=compiler-rt \
        -DCLANG_DEFAULT_UNWINDLIB=libunwind \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_C_COMPILER=clang \
        -DLLVM_ENABLE_LIBCXX=ON \
        -DLLVM_ENABLE_LLD=ON \
        -DLIBOMP_LIBFLAGS="-lm" \
        -DCMAKE_INSTALL_PREFIX="$SYSROOT" \
        -DCMAKE_INSTALL_RPATH="\$ORIGIN/../lib/;\$ORIGIN/../lib/$(uname -m)-unknown-linux-gnu/" \
        ../llvm

    ninja
    ninja install
    cd ../..
    rm -rf llvm-project
}

function install_openssl() {
    wget https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_${OPENSSL_VERSION}.tar.gz 
    tar xvf OpenSSL_${OPENSSL_VERSION}.tar.gz 
    cd openssl-OpenSSL_${OPENSSL_VERSION}

    ./config                                \
        -fPIC                               \
        no-shared                           \
        no-afalgeng                         \
        --prefix="$SYSROOT"                 \
        --openssldir="$SYSROOT"             \
        -static
    
     NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
     make -j ${NPROC}
     make install_sw install_ssldirs

     cd .. 
     rm -rf openssl-OpenSSL_${OPENSSL_VERSION}
     rm -rf OpenSSL_${OPENSSL_VERSION}.tar.gz
}

function install_go() {
    wget https://dl.google.com/go/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz 
    tar -C "$SYSROOT" -xzvf go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
    rm -rf go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
}

function install_curl() {
    NPROC=$(nproc || grep -c ^processor /proc/cpuinfo) 
    ENCODED_VERSION=$(echo $CURL_VERSION | tr . _)
    wget https://github.com/curl/curl/releases/download/curl-$ENCODED_VERSION/curl-$CURL_VERSION.tar.gz && \
    tar zxf curl-$CURL_VERSION.tar.gz && \
    cd curl-$CURL_VERSION && \
    ./configure --with-openssl="$SYSROOT" --disable-shared --prefix=$SYSROOT && \
    make -j ${NPROC} && \
    make install && \
    cd .. && \
    rm -rf curl-$CURL_VERSION && \
    rm -rf curl-$CURL_VERSION.tar.gz
}

function install_ccache() {
    wget "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz"
    tar xvaf "ccache-${CCACHE_VERSION}.tar.gz" && rm -rf "ccache-${CCACHE_VERSION}.tar.gz"
    mkdir -p "ccache-${CCACHE_VERSION}/build"
    cd "ccache-${CCACHE_VERSION}/build" || exit 1
    cmake .. -DCMAKE_BUILD_TYPE=Release \
      -DZSTD_FROM_INTERNET=ON \
      -DHIREDIS_FROM_INTERNET=ON \
      -DENABLE_TESTING=OFF \
      -DCMAKE_INSTALL_RPATH="\$ORIGIN/../lib/;\$ORIGIN/../lib/$(uname -m)-unknown-linux-gnu/" \
      -DCMAKE_INSTALL_PREFIX="$SYSROOT" \
      -GNinja
    ninja && ninja install
    cd ../..
    rm -rf "ccache-$CCACHE_VERSION"
}

mkdir -p $SYSROOT

install_cmake 
install_llvm
install_openssl
install_go
install_curl
install_ccache

# some extra steps
if [[ -e /usr/lib64/libtinfo.so.5 ]]; then
    cp /usr/lib64/libtinfo.so.5 "$SYSROOT/lib"
fi
