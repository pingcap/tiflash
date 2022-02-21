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

CMAKE_BUILD_TYPE=Debug
BUILD_BRANCH=${BUILD_BRANCH:-master}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-utcov-llvm.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')

if [[ ${UPDATE_CCACHE} != "false" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
  exit 0
fi
