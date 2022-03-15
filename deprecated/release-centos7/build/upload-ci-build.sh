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

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}
source ${SCRIPTPATH}/env.sh
TAR_BIN_URI="${TIFLASH_CI_BUILD_URI_PREFIX}/${PULL_ID}/tiflash.tar.gz"
COMMIT_HASH_FILE_URI="${TIFLASH_CI_BUILD_URI_PREFIX}/${PULL_ID}/commit-hash"

build_dir="$SRCPATH/release-centos7"
cd ${build_dir}
tar -zcf tiflash.tar.gz ./tiflash
MD5_SUM=$(md5sum ./tiflash.tar.gz | awk '{ print $1 }')
echo "${CI_BUILD_INFO_PRE_FIX}" >./commit-hash
echo "${MD5_SUM}" >>./commit-hash
echo "${COMMIT_HASH}" >>./commit-hash

curl -F ${TAR_BIN_URI}=@tiflash.tar.gz http://fileserver.pingcap.net/upload
curl -F ${COMMIT_HASH_FILE_URI}=@commit-hash http://fileserver.pingcap.net/upload
