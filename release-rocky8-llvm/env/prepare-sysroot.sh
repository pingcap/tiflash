#!/usr/bin/env bash
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -ueox pipefail

CMAKE_VERSION="3.24.2"
ARCH=$(uname -m)
LLVM_VERSION="17.0.6"
CCACHE_VERSION="4.5.1"
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SYSROOT="${SCRIPTPATH}/sysroot"
OPENSSL_VERSION="1_1_1w"

function install_cmake() {
    wget "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${ARCH}.sh"
    sh cmake-${CMAKE_VERSION}-linux-${ARCH}.sh --prefix="${SYSROOT}" --skip-license --exclude-subdir
    rm -rf cmake-${CMAKE_VERSION}-linux-${ARCH}.sh
}

function install_llvm() {
    git clone https://github.com/llvm/llvm-project --depth=1 -b "llvmorg-${LLVM_VERSION}"

    mkdir -p llvm-project/build
    cd llvm-project/build

    # `bolt` is enabled; https://github.com/llvm/llvm-project/tree/main/bolt
    cmake -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DLLVM_ENABLE_PROJECTS="clang;lld;polly;clang-tools-extra;bolt" \
        -DLLVM_ENABLE_RUNTIMES="compiler-rt;libcxx;libcxxabi;libunwind;openmp" \
        -DLLVM_TARGETS_TO_BUILD=Native \
        -DCOMPILER_RT_USE_BUILTINS_LIBRARY=ON \
        -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
        -DCLANG_DEFAULT_LINKER=lld \
        -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_C_COMPILER=clang \
        -DLLVM_ENABLE_LIBCXX=ON \
        -DLLVM_ENABLE_LLD=ON \
        -DLIBOMP_LIBFLAGS="-lm" \
        -DCMAKE_INSTALL_PREFIX="${SYSROOT}" \
        -DCMAKE_INSTALL_RPATH="\$ORIGIN/../lib/;\$ORIGIN/../lib/$(uname -m)-unknown-linux-gnu/" \
        ../llvm

    ninja
    ninja install
    cd ../..
    rm -rf llvm-project
}

function install_openssl() {
    wget "https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_${OPENSSL_VERSION}.tar.gz"
    tar xvf OpenSSL_${OPENSSL_VERSION}.tar.gz
    cd openssl-OpenSSL_${OPENSSL_VERSION}

    ./config                                \
        -fPIC                               \
        no-shared                           \
        no-afalgeng                         \
        --prefix="${SYSROOT}"                 \
        --openssldir="${SYSROOT}"             \
        -static

     NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
     make -j ${NPROC}
     make install_sw install_ssldirs

     cd ..
     rm -rf openssl-OpenSSL_${OPENSSL_VERSION}
     rm -rf OpenSSL_${OPENSSL_VERSION}.tar.gz
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
      -DCMAKE_INSTALL_PREFIX="${SYSROOT}" \
      -GNinja
    ninja && ninja install
    cd ../..
    rm -rf "ccache-$CCACHE_VERSION"
}

mkdir -p ${SYSROOT}

install_cmake 
install_llvm
install_openssl
install_ccache

# some extra steps
if [[ -e /usr/lib64/libtinfo.so.5 ]]; then
    cp /usr/lib64/libtinfo.so.5 "${SYSROOT}/lib"
fi
