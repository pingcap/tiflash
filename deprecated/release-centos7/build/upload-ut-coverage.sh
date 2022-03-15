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


set -ueox pipefail

SRCPATH=/build/tics
BUILD_DIR=/build/release-centos7/build-release
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

gcovr --xml -r ${SRCPATH} \
    -e "/usr/include/*" -e "/usr/local/*" -e "/usr/lib/*" \
    -e "${SRCPATH}/contrib/*" \
    -e "${SRCPATH}/dbms/src/Debug/*" \
    -e "${SRCPATH}/dbms/src/Client/*" \
    --object-directory=${BUILD_DIR} -o /tmp/tiflash_gcovr_coverage.xml -j ${NPROC} -s >/tmp/tiflash_gcovr_coverage.res

cat /tmp/tiflash_gcovr_coverage.res
