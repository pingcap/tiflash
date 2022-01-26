#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install CCACHE for CI/CD.

function install_ccache() {
    # $1 : version
    wget "https://github.com/ccache/ccache/releases/download/v$1/ccache-$1.tar.gz"
    tar xvaf "ccache-$1.tar.gz" && rm -rf "ccache-$1.tar.gz"
    mkdir -p "ccache-$1/build"
    cd "ccache-$1/build" || exit 1
    cmake .. -DCMAKE_BUILD_TYPE=Release \
      -DZSTD_FROM_INTERNET=ON \
      -DHIREDIS_FROM_INTERNET=ON \
      -DENABLE_TESTING=OFF \
      -DCMAKE_INSTALL_PREFIX="/usr/local" \
      -GNinja
    ninja && ninja install
    cd ../..
    rm -rf "ccache-$1"
}
