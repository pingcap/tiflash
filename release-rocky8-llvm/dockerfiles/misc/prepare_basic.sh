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


# Prepare basic environment for CI/CD.

function prepare_basic() {
    dnf upgrade-minimal -y
    dnf install -y epel-release
    dnf config-manager --set-enabled powertools
    dnf install -y \
         libtool \
         libtool-ltdl-devel \
         python3-devel \
         bzip2 \
         chrpath
    dnf install -y curl git perl wget cmake3 gettext glibc-static zlib-devel diffutils ninja-build gcc-toolset-10
    dnf install -y 'perl(Data::Dumper)'
    dnf clean all -y
}
