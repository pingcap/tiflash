#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}

ENABLE_FORMAT_CHECK=${ENABLE_FORMAT_CHECK:-false}
if [[ "${ENABLE_FORMAT_CHECK}" == "true" ]]; then
  python3 ${SRCPATH}/format-diff.py --repo_path "${SRCPATH}" --check_formatted --diff_from $(git merge-base origin/${BUILD_BRANCH} HEAD) --dump_diff_files_to "/tmp/tiflash-diff-files.json"
fi

INSTALL_DIR=${INSTALL_DIR:-"$SRCPATH/release-centos7-llvm/tiflash"} # use original path

CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
BUILD_BRANCH=${BUILD_BRANCH:-master}
CI_CCACHE_USED_SRCPATH="/build/tics"
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  mkdir -p /build && cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
fi

# https://cd.pingcap.net/blue/organizations/jenkins/build_tiflash_multi_branch/activity/
# Each time after a new commit merged into target branch, a task about nightly build will be triggered.
# BUILD_UPDATE_DEBUG_CI_CCACHE is set true in order to build and upload ccache.
if [[ "${BUILD_UPDATE_DEBUG_CI_CCACHE}" != "false" ]]; then
  echo "====== begin to build & upload ccache for ci debug build ======"
  UPDATE_CCACHE=true BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-prepare.sh
  BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug NPROC=${NPROC} INSTALL_DIR=${INSTALL_DIR} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-build.sh
  BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug INSTALL_DIR=${INSTALL_DIR} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-finish.sh
  echo "======  finish build & upload ccache for ci debug build  ======"
fi

BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-prepare.sh
BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} NPROC=${NPROC} INSTALL_DIR=${INSTALL_DIR} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-build.sh
BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} INSTALL_DIR=${INSTALL_DIR} sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-finish.sh
