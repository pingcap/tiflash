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


# Install Go for CI/CD.
# Require: wget, tar

function install_go() {
    # $1: go_version
    # $2: go_arch
    wget https://dl.google.com/go/go$1.linux-$2.tar.gz 
    tar -C /usr/local -xzvf go$1.linux-$2.tar.gz
    rm -rf go$1.linux-$2.tar.gz
}
