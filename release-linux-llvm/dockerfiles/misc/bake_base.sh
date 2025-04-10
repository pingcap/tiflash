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

# You should only contain the basic environment and tools that are needed for building LLVM.

set -eox pipefail
source $(cd $(dirname "$0"); pwd -P)/_bake_include.sh

dnf upgrade-minimal -y
dnf install -y epel-release
dnf config-manager --set-enabled powertools
dnf install -y \
     libtool \
     libtool-ltdl-devel \
     python3-devel \
     libcurl-devel \
     bzip2 \
     chrpath
dnf install -y curl git perl wget cmake3 gettext glibc-static zlib-devel diffutils ninja-build gcc-toolset-10
dnf install -y 'perl(Data::Dumper)'
dnf clean all -y


# Install cmake for CI/CD.
# Require: wget
# CMake License: https://gitlab.kitware.com/cmake/cmake/raw/master/Copyright.txt
function install_cmake() {
    # $1: cmake_version
    wget https://github.com/Kitware/CMake/releases/download/v$1/cmake-$1-linux-${arch}.sh
    mkdir -p /opt/cmake
    sh cmake-$1-linux-${arch}.sh --prefix=/opt/cmake --skip-license --exclude-subdir
    rm -rf cmake-$1-linux-${arch}.sh
}

install_cmake "3.24.2"
