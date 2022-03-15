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


set -euo pipefail

wget https://www.openssl.org/source/old/1.1.0/openssl-1.1.0f.tar.gz
tar xvf openssl-1.1.0f.tar.gz
cd openssl-1.1.0f
./config -fPIC no-shared no-afalgeng --prefix=/usr/local/opt/openssl --openssldir=/usr/local/opt/openssl -static
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
make -j ${NPROC}
make install
