#!/bin/bash

set -ueox pipefail
SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
source ${SCRIPTPATH}/env.sh
SRCPATH=/build/tics
BUILD_DIR=/build/release-centos7-llvm/build-release
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
COV_URI="${TIFLASH_CI_BUILD_URI_PREFIX}/${PULL_ID}/coverage-report.tar.gz"

llvm-profdata merge -sparse /tiflash/profile/*.profraw -o /tiflash/profile/merged.profdata 

llvm-cov export \
    /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon \
    --format=lcov \
    --instr-profile /tiflash/profile/merged.profdata \
    --ignore-filename-regex "/usr/include/.*" \
    --ignore-filename-regex "/usr/local/.*" \
    --ignore-filename-regex "/usr/lib/.*" \
    --ignore-filename-regex "${SRCPATH}/contrib/.*" \
    --ignore-filename-regex "${SRCPATH}/dbms/src/Debug/.*" \
    --ignore-filename-regex "${SRCPATH}/dbms/src/Client/.*" \
    > /tiflash/profile/lcov.info

mkdir -p /tiflash/report
genhtml /tiflash/profile/lcov.info -o /tiflash/report/
cd /tiflash
tar -czf coverage-report.tar.gz report
curl -F ${COV_URI}=@coverage-report.tar.gz http://fileserver.pingcap.net/upload

cd $SRCPATH
git fetch origin
SOURCE_DELTA=$(git diff --name-only origin/HEAD | grep -E '.*\.(cpp|h|hpp|cc|c)$')
llvm-cov report /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon -instr-profile /tiflash/profile/merged.profdata $SOURCE_DELTA > /tiflash/profile/diff-coverage