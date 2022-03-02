#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install cmake for CI/CD.
# Require: wget
# CMake License: https://gitlab.kitware.com/cmake/cmake/raw/master/Copyright.txt

function install_cmake() {
    # $1: cmake_version
    # $2: cmake_arch
    wget https://github.com/Kitware/CMake/releases/download/v$1/cmake-$1-linux-$2.sh
    mkdir -p /opt/cmake
    sh cmake-$1-linux-$2.sh --prefix=/opt/cmake --skip-license --exclude-subdir
    rm -rf cmake-$1-linux-$2.sh
}