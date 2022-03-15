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

VERSION="3.10"
COMPLETE_VERSION="3.10.2"

cd ~
wget "https://download.pingcap.org/cmake-${COMPLETE_VERSION}-Linux-x86_64.tar.gz"
tar zxvf cmake-${COMPLETE_VERSION}-Linux-x86_64.tar.gz

rm cmake-${COMPLETE_VERSION}-Linux-x86_64.tar.gz
yum remove cmake -y

ln -sf ~/cmake-${COMPLETE_VERSION}-Linux-x86_64/bin/ccmake /usr/bin/ccmake
ln -sf ~/cmake-${COMPLETE_VERSION}-Linux-x86_64/bin/cmake /usr/bin/cmake
ln -sf ~/cmake-${COMPLETE_VERSION}-Linux-x86_64/bin/cmake-gui /usr/bin/cmake-gui
ln -sf ~/cmake-${COMPLETE_VERSION}-Linux-x86_64/bin/cpack /usr/bin/cpack
ln -sf ~/cmake-${COMPLETE_VERSION}-Linux-x86_64/bin/ctest /usr/bin/ctest

cmake --version
