#!/usr/bin/env bash

# Tiflash-Proxy Building script
# Copyright PingCAP, Inc
# THIS SCRIPT SHOULD ONLY BE INVOKED IN DOCKER

set -ueox pipefail

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"
SRCPATH=$(
    cd ${SCRIPTPATH}/../..
    pwd -P
)
PATH=$PATH:/root/.cargo/bin

cd ${SRCPATH}/contrib/tiflash-proxy

# rocksdb, grpc build is configured with WERROR
export CFLAGS="-w" 
export CXXFLAGS="-w"

# override cc-rs default STL lib
export CXXSTDLIB="c++"
export CMAKE="/opt/cmake/bin/cmake"
# Keep C++ runtime libs to be linked dynamically (ODR safe). 
# Rustc seems resolve static lib on default even when the
# above flag is set to "dylib=c++"

if [ -f /.dockerenv ]; then
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libc++abi.a
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libc++.a
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libunwind.a
fi

make release
