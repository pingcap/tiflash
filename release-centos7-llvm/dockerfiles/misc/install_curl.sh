#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install cURL for CI/CD.
# Require: wget, tar, make

function install_curl() {
    # $1: curl_version
    NPROC=$(nproc || grep -c ^processor /proc/cpuinfo) 
    ENCODED_VERSION=$(echo $1 | tr . _)
    wget https://github.com/curl/curl/releases/download/curl-$ENCODED_VERSION/curl-$1.tar.gz && \
    tar zxf curl-$1.tar.gz && \
    cd curl-$1 && \
    ./configure --with-openssl=/usr/local/opt/openssl --disable-shared && \
    make -j ${NPROC} && \
    make install && \
    cd .. && \
    rm -rf curl-$1 && \
    rm -rf curl-$1.tar.gz
}