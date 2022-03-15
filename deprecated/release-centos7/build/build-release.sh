#!/bin/bash
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


CMAKE_PREFIX_PATH=$1

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)

install_dir="$SRCPATH/release-centos7/tiflash"
if [[ -d "$install_dir" ]]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi

${SCRIPTPATH}/build-tiflash-proxy.sh
${SCRIPTPATH}/build-tiflash-release.sh "" "${CMAKE_PREFIX_PATH}"
