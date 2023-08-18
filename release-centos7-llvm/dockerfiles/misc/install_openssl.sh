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