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


# Install cmake for CI/CD.
# Require: wget
# CMake License: https://gitlab.kitware.com/cmake/cmake/raw/master/Copyright.txt

function install_cmake() {
    # $1: cmake_version
    arch=$(uname -m)
    wget https://github.com/Kitware/CMake/releases/download/v$1/cmake-$1-linux-${arch}.sh
    mkdir -p /opt/cmake
    sh cmake-$1-linux-${arch}.sh --prefix=/opt/cmake --skip-license --exclude-subdir
    rm -rf cmake-$1-linux-${arch}.sh
}
