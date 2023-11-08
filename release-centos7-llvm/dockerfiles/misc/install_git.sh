#!/usr/bin/env bash
# Copyright 2022 PingCAP, Ltd.
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

# Install git for CI/CD.
# Require: openssl

function install_git() {
    # $1: git version
    wget -O git.tar.gz https://github.com/git/git/archive/refs/tags/v$1.tar.gz
    mkdir git && cd git
    tar -xzvf ../git.tar.gz --strip-components=1
    make configure
    ./configure --with-openssl $OPENSSL_ROOT_DIR --prefix /usr/local
    make -j
    make install
    cd ..
    rm -rf git git.tar.gz
}
