#!/usr/bin/env bash

# TiFlash-Proxy Building script
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

make release
