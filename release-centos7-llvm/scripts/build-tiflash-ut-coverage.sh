#!/bin/bash

set -ueox pipefail

scriptpath="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $scriptpath/../..
  pwd -P
)}

BUILD_BRANCH=${BUILD_BRANCH:-master}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}

source ${SRCPATH}/release-centos7-llvm/scripts/env.sh

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  mkdir -p /build && cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
fi

UPDATE_CCACHE=${UPDATE_CCACHE} BUILD_BRANCH=${BUILD_BRANCH} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/tiflash-ut-coverage-prepare.sh
NPROC=${NPROC} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/tiflash-ut-coverage-build.sh
UPDATE_CCACHE=${UPDATE_CCACHE} BUILD_BRANCH=${BUILD_BRANCH} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/tiflash-ut-coverage-finish.sh
