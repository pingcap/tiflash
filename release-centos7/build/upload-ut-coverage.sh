#!/bin/bash

set -ueox pipefail

SRCPATH=/build/tics
BUILD_DIR=/build/release-centos7/build-release
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

gcovr --xml -r ${SRCPATH} -e "/usr/include/*" -e "/usr/local/*" -e "/usr/lib/*" -e "${SRCPATH}/contrib/*" --object-directory=${BUILD_DIR} -o /tmp/tiflash_gcovr_coverage.xml -j ${NPROC} -s >/tmp/tiflash_gcovr_coverage.res

cat /tmp/tiflash_gcovr_coverage.res
