#!/bin/bash

set -ueox pipefail

SRCPATH=/build/tics
BUILD_DIR=/build/release-centos7-llvm/build-release
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

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
genhtml /tiflash/profile/lcov.info -o /tiflash/report