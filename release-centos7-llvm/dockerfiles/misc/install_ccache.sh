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
    mkdir -p /usr/lib64/ccache && ln -s `which ccache` /usr/lib64/ccache/clang &&  ln -s `which ccache` /usr/lib64/ccache/clang++
    cd ../..
    rm -rf "ccache-$1"
}
