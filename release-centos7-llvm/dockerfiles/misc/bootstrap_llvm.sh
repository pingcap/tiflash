#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.


# Boostrap LLVM envriroment for CI/CD.
# Require: git, ninja, cmake, compiler(devtoolset-10)
# LLVM License: https://releases.llvm.org/13.0.0/LICENSE.TXT

function bootstrap_llvm() {
    # $1: llvm_version
    source scl_source enable devtoolset-10
    git clone https://github.com/llvm/llvm-project --depth=1 -b llvmorg-$1
   
    mkdir -p llvm-project/build
    cd llvm-project/build

    cmake -DCMAKE_BUILD_TYPE=Release \
        -GNinja \
        -DLLVM_ENABLE_PROJECTS="clang;lld" \
        -DLLVM_ENABLE_RUNTIMES="compiler-rt;libcxx;libcxxabi;libunwind" \
        -DLLVM_TARGETS_TO_BUILD=Native \
        -DCOMPILER_RT_USE_BUILTINS_LIBRARY=ON \
        -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
        ../llvm
    
    ninja
    ninja install

    cd ../..
    rm -rf llvm-project/build
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
        ../llvm

    ninja
    ninja install
    cd ../..
    rm -rf llvm-project
}