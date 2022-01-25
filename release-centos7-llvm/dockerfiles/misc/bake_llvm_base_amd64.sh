#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Build CI/CD image

function bake_llvm_base_amd64() {
    export PATH="/opt/cmake/bin:/usr/local/bin/:${PATH}"
    export LIBRARY_PATH="/usr/local/lib/x86_64-unknown-linux-gnu/:${LIBRARY_PATH:+LIBRARY_PATH:}"
    export LD_LIBRARY_PATH="/usr/local/lib/x86_64-unknown-linux-gnu/:${LD_LIBRARY_PATH:+LD_LIBRARY_PATH:}"
    export CPLUS_INCLUDE_PATH="/usr/local/include/x86_64-unknown-linux-gnu/c++/v1/:${CPLUS_INCLUDE_PATH:+CPLUS_INCLUDE_PATH:}"
    SCRIPTPATH=$(cd $(dirname "$0"); pwd -P)

    # Basic Environment
    source $SCRIPTPATH/prepare_basic.sh
    prepare_basic
    
    # CMake
    source $SCRIPTPATH/install_cmake.sh
    install_cmake "3.22.1" "x86_64"

    # LLVM
    source $SCRIPTPATH/bootstrap_llvm.sh
    bootstrap_llvm "13.0.0"
    export CC=clang
    export CXX=clang++
    export LD=ld.lld

    # OpenSSL
    source $SCRIPTPATH/install_openssl.sh
    install_openssl "1_1_1l"
    export OPENSSL_ROOT_DIR="/usr/local/opt/openssl"

    # Go
    source $SCRIPTPATH/install_go.sh
    install_go "1.17" "amd64"
    export PATH="$PATH:/usr/local/go/bin"

    # Rust
    source $SCRIPTPATH/install_rust.sh
    install_rust 
    source $HOME/.cargo/env

    # Curl
    source $SCRIPTPATH/install_curl.sh
    install_curl "7.80.0"

    # ccache
    source $SCRIPTPATH/install_ccache.sh
    install_ccache "4.5.1"
}

bake_llvm_base_amd64
