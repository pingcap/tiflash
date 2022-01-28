#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install openssl for CI/CD.
# Require: wget, tar

function install_openssl() {
    # $1: openssl_version
    wget https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_$1.tar.gz 
    tar xvf OpenSSL_$1.tar.gz 
    cd openssl-OpenSSL_$1

    ./config                                \
        -fPIC                               \
        no-shared                           \
        no-afalgeng                         \
        --prefix=/usr/local/opt/openssl     \
        --openssldir=/usr/local/opt/openssl \
        -static
    
     NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
     make -j ${NPROC}
     make install_sw install_ssldirs

     cd .. 
     rm -rf openssl-OpenSSL_$1
     rm -rf OpenSSL_$1.tar.gz
}