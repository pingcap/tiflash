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


# Install Rust for CI/CD.
# Require: curl

function install_rust() {
    curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly
    SCRIPTPATH=$(cd $(dirname "$0"); pwd -P)
    mkdir -p $HOME/.cargo/
    cp $SCRIPTPATH/cargo-config $HOME/.cargo/config
}
