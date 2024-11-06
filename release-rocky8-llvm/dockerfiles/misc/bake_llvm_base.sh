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


# Build CI/CD image

function bake_llvm_base() {
    arch=$(uname -m)
    export PATH="/opt/cmake/bin:/usr/local/bin/:${PATH}"
    export LIBRARY_PATH="/usr/local/lib/${arch}-unknown-linux-gnu/:${LIBRARY_PATH:+LIBRARY_PATH:}"
    export LD_LIBRARY_PATH="/usr/local/lib/${arch}-unknown-linux-gnu/:${LD_LIBRARY_PATH:+LD_LIBRARY_PATH:}"
    export CPLUS_INCLUDE_PATH="/usr/local/include/${arch}-unknown-linux-gnu/c++/v1/:${CPLUS_INCLUDE_PATH:+CPLUS_INCLUDE_PATH:}"
    SCRIPTPATH=$(cd $(dirname "$0"); pwd -P)

    # Basic Environment
    source $SCRIPTPATH/prepare_basic.sh
    prepare_basic
    
    # CMake
    source $SCRIPTPATH/install_cmake.sh
    install_cmake "3.24.2"

    # LLVM
    source $SCRIPTPATH/bootstrap_llvm.sh
    bootstrap_llvm "17.0.6"
    export CC=clang
    export CXX=clang++
    export LD=ld.lld

    # OpenSSL
    source $SCRIPTPATH/install_openssl.sh
    install_openssl "1_1_1w"
    export OPENSSL_ROOT_DIR="/usr/local/opt/openssl"

    # Git
    source $SCRIPTPATH/install_git.sh
    install_git "2.40.1"

    # Rust
    source $SCRIPTPATH/install_rust.sh
    install_rust 
    source $HOME/.cargo/env

    # ccache
    source $SCRIPTPATH/install_ccache.sh
    install_ccache "4.10.2"
}

bake_llvm_base
