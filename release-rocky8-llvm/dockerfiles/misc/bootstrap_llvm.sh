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

INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}

# Boostrap LLVM envriroment for CI/CD.
# Require: git, ninja, cmake, compiler(gcc-toolset-100)
# LLVM License: https://releases.llvm.org/17.0.6/LICENSE.TXT

function bootstrap_llvm() {
    # $1: llvm_version
    source /opt/rh/gcc-toolset-10/enable
    git clone https://github.com/llvm/llvm-project --depth=1 -b llvmorg-$1
   
    mkdir -p llvm-project/build
    cd llvm-project/build

    cmake -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
        -DLLVM_ENABLE_PROJECTS="clang;lld" \
        -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi" \
        -DLLVM_TARGETS_TO_BUILD=Native \
        ../llvm
    
    ninja
    ninja install

    cd ../..
    rm -rf llvm-project/build
    mkdir -p llvm-project/build
    cd llvm-project/build

    cmake -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
        -DLLVM_ENABLE_PROJECTS="clang;lld;polly;clang-tools-extra;bolt" \
        -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi;openmp;compiler-rt" \
        -DLLVM_TARGETS_TO_BUILD=Native \
        -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
        -DLLVM_LIBDIR_SUFFIX=64 \
        -DCLANG_DEFAULT_LINKER=lld \
        -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_C_COMPILER=clang \
        -DLLVM_ENABLE_LIBCXX=ON \
        -DLLVM_ENABLE_LLD=ON \
        -DLIBOMP_LIBFLAGS="-lm" \
        ../llvm

    ninja
    ninja install
    cd ../..
    rm -rf llvm-project
    
    echo "/usr/local/lib/$(uname -m)-unknown-linux-gnu" | tee /etc/ld.so.conf.d/llvm.conf
    ldconfig
}
