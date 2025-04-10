#!/usr/bin/env bash
# Copyright 2025 PingCAP, Inc.
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

set -eox pipefail
source $(cd $(dirname "$0"); pwd -P)/_bake_include.sh

# OpenSSL
source $SCRIPTPATH/install_openssl.sh
install_openssl "1_1_1w"
export OPENSSL_ROOT_DIR="/usr/local/opt/openssl"

# Rust
source $SCRIPTPATH/install_rust.sh
install_rust "nightly-2024-12-12"
source $HOME/.cargo/env

# ccache
source $SCRIPTPATH/install_ccache.sh
install_ccache "4.10.2"

# some other required devtools
dnf upgrade-minimal -y
dnf install -y protobuf-compiler xz xz-libs glibc-langpack-en hostname wget procps-ng python27 clang-tools-extra-17.0.6
dnf install -y https://dev.mysql.com/get/mysql80-community-release-el8-1.noarch.rpm
dnf module disable mysql -y
dnf install -y mysql-community-client --nogpgcheck
dnf clean all -y
ln -s /usr/bin/python2 /usr/bin/python
ln -s /usr/bin/pip2 /usr/bin/pip
