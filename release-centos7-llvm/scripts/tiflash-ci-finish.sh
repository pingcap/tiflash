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

echo "INSTALL_DIR=${INSTALL_DIR}"

CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
BUILD_BRANCH=${BUILD_BRANCH:-master}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}

CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-llvm.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')

# Download prebuilt tiflash proxy to accelerate CI build.
TIFLASH_PROXY_SRC=${SRCPATH}/contrib/tiflash-proxy
TIFLASH_PROXY_TAR_DIR=${TIFLASH_PROXY_SRC}/target/release

if [[ -f /tmp/build_tiflash_proxy_flag ]]; then
  proxy_git_hash=$(tail -n -1 /tmp/build_tiflash_proxy_flag)
  cd ${INSTALL_DIR}
  curl -F builds/pingcap/tiflash-proxy/${proxy_git_hash}-llvm/libtiflash_proxy.so=@libtiflash_proxy.so http://fileserver.pingcap.net/upload
  rm -rf /tmp/build_tiflash_proxy_flag
fi

if [[ ${UPDATE_CCACHE} != "false" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
fi
