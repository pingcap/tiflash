#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Prepare python environment for CI/CD.

function prepare_python() {
    # $1: grpcio version
    source scl_source enable devtoolset-10
    export CC=gcc
    export CXX=g++
    pip3 install wheel # this may be a dependency for other packages
    pip3 install \
        pybind11 \
        pyinstaller \
        dnspython \
        uri \
        requests \
        urllib3 \
        toml \
        setuptools \
        etcd3 \
        grpcio==$1
}
