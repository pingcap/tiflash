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
  cd ${SCRIPTPATH}/../..
  pwd -P
)}

source ${SRCPATH}/release-centos7-llvm/scripts/env.sh

SRCPATH=${CI_CCACHE_USED_SRCPATH}
BUILD_DIR={BUILD_UT_DIR}

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

llvm-profdata merge -sparse /tiflash/profile/*.profraw -o /tiflash/profile/merged.profdata 

llvm-cov export \
    /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon \
    --format=lcov \
    --instr-profile /tiflash/profile/merged.profdata \
    --ignore-filename-regex "/usr/include/.*" \
    --ignore-filename-regex "/usr/local/.*" \
    --ignore-filename-regex "/usr/lib/.*" \
    --ignore-filename-regex ".*/contrib/.*" \
    --ignore-filename-regex ".*/dbms/src/Debug/.*" \
    --ignore-filename-regex ".*/dbms/src/Client/.*" \
    > /tiflash/profile/lcov.info

mkdir -p /tiflash/report
genhtml /tiflash/profile/lcov.info -o /tiflash/report/
cd /tiflash
tar -czf coverage-report.tar.gz report

cd $SRCPATH
COMMIT_HASH_BASE=$(git merge-base origin/${BUILD_BRANCH} HEAD)
SOURCE_DELTA=$(git diff --name-only ${COMMIT_HASH_BASE} | { grep -E '.*\.(cpp|h|hpp|cc|c)$' || true; })
echo '### Coverage for changed files' > /tiflash/profile/diff-coverage
echo '```' >> /tiflash/profile/diff-coverage

if [[ -z ${SOURCE_DELTA} ]]; then
	echo 'no c/c++ source change detected' >> /tiflash/profile/diff-coverage
else
	llvm-cov report /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon -instr-profile /tiflash/profile/merged.profdata $SOURCE_DELTA > "/tiflash/profile/diff-for-delta"
  if [[ $(wc -l "/tiflash/profile/diff-for-delta" | awk -e '{printf $1;}') -gt 32 ]]; then
    echo 'too many lines from llvm-cov, please refer to full report instead' >> /tiflash/profile/diff-coverage
  else
    cat /tiflash/profile/diff-for-delta >> /tiflash/profile/diff-coverage
  fi
fi

echo '```' >> /tiflash/profile/diff-coverage
echo '' >> /tiflash/profile/diff-coverage
echo '### Coverage summary' >> /tiflash/profile/diff-coverage
echo '```' >> /tiflash/profile/diff-coverage
llvm-cov report \
    --summary-only \
    --show-region-summary=false \
    --show-branch-summary=false \
    --ignore-filename-regex "/usr/include/.*" \
    --ignore-filename-regex "/usr/local/.*" \
    --ignore-filename-regex "/usr/lib/.*" \
    --ignore-filename-regex ".*/contrib/.*" \
    --ignore-filename-regex ".*/dbms/src/Debug/.*" \
    --ignore-filename-regex ".*/dbms/src/Client/.*" \
    /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon -instr-profile /tiflash/profile/merged.profdata | \
    grep -E "^(TOTAL|Filename)" | \
    cut -d" " -f2- | sed -e 's/^[[:space:]]*//' | sed -e 's/Missed\ /Missed/g' | column -t >> /tiflash/profile/diff-coverage
echo '```' >> /tiflash/profile/diff-coverage
echo '' >> /tiflash/profile/diff-coverage
echo "[full coverage report](https://ci-internal.pingcap.net/job/tics_ghpr_unit_test/${BUILD_NUMBER}/artifact/coverage-report.tar.gz) (for internal network access only)" >> /tiflash/profile/diff-coverage
cat /tiflash/profile/diff-coverage
