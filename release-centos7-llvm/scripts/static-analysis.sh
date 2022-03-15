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


ENABLE_CLANG_TIDY_CHECK=${ENABLE_CLANG_TIDY_CHECK:-true}

set -ueox pipefail

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
source ${SRCPATH}/release-centos7-llvm/scripts/env.sh

if [[ "${ENABLE_CLANG_TIDY_CHECK}" == "true" ]]; then
  BUILD_DIR="${SRCPATH}/${BUILD_DIR_SUFFIX}"

  cd ${BUILD_DIR}
  cmake "${SRCPATH}" \
    -DENABLE_TESTS=0 \
    -DCMAKE_BUILD_TYPE=RELWITHDEBINFO \
    -DUSE_CCACHE=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DRUN_HAVE_STD_REGEX=0 \
    -GNinja
  python3 ${SRCPATH}/release-centos7-llvm/scripts/fix_compile_commands.py \
          --file_path=${BUILD_DIR}/compile_commands.json \
          --load_diff_files_from "/tmp/tiflash-diff-files.json"
  python3 ${SRCPATH}/release-centos7-llvm/scripts/run-clang-tidy.py -p ${BUILD_DIR} -j ${NPROC} --files ".*/tics/dbms/*"
fi
