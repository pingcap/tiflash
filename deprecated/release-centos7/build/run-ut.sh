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


scriptpath="$(
    cd "$(dirname "$0")"
    pwd -P
)"

SRCPATH=${1:-$(
    cd $scriptpath/../..
    pwd -P
)}

set -x

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
OUTPUT_XML=${OUTPUT_XML:-false}

INSTALL_DIR=${INSTALL_DIR:-"/build/release-centos7/tiflash"}

rm -rf /tests && ln -s ${SRCPATH}/tests /tests
rm -rf /tiflash && ln -s ${INSTALL_DIR} /tiflash

source /tests/docker/util.sh

show_env

ENV_VARS_PATH=/tests/docker/_env.sh OUTPUT_XML=${OUTPUT_XML} NPROC=${NPROC} /tests/run-gtest.sh
