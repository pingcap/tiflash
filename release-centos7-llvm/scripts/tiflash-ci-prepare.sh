#!/bin/bash

command -v ccache >/dev/null 2>&1
ccache_major=$(ccache --version | head - -n 1 | tr '.' ' ' | awk -e '{ print $3 }')
if [[ $? != 0 || $ccache_major -lt 4 ]]; then
  echo "try to install ccache"
  curl -o /usr/local/bin/ccache http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/ccache-4.5.1
  chmod +x /usr/local/bin/ccache
else
  echo "ccache has been installed"
fi

command -v clang-format >/dev/null 2>&1
if [[ $? != 0 ]]; then
  curl -o "/usr/local/bin/clang-format" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/clang-format-12
  chmod +x "/usr/local/bin/clang-format"
else
  echo "clang-format has been installed"
fi
clang-format --version

command -v clang-tidy >/dev/null 2>&1
if [[ $? != 0 ]]; then
  curl -o "/usr/local/bin/clang-tidy" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/clang-tidy-12
  chmod +x "/usr/local/bin/clang-tidy"
else
  echo "clang-tidy has been installed"
fi
clang-tidy --version

set -ueox pipefail

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}

CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
BUILD_BRANCH=${BUILD_BRANCH:-master}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-llvm.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')

rm -rf "${SRCPATH}/.ccache"
cache_file="${SRCPATH}/ccache.tar"
rm -rf "${cache_file}"
curl -o "${cache_file}" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}
cache_size=$(ls -l "${cache_file}" | awk '{print $5}')
min_size=$((1024000))
if [[ ${cache_size} -gt ${min_size} ]]; then
  echo "try to use ccache to accelerate compile speed"
  cd "${SRCPATH}"
  tar -xf ccache.tar
fi
ccache -o cache_dir="${SRCPATH}/.ccache"
ccache -o max_size=2G
ccache -o limit_multiple=0.99
ccache -o hash_dir=false
ccache -o compression=true
ccache -o compression_level=6
if [[ ${UPDATE_CCACHE} == "false" ]]; then
  ccache -o read_only=true
else
  ccache -o read_only=false
fi
ccache -z

TIFLASH_PROXY_BIN_PATH=/tmp/libtiflash_proxy.so
rm -rf ${TIFLASH_PROXY_BIN_PATH}

cd ${SRCPATH}/contrib/tiflash-proxy
proxy_git_hash=$(git log -1 --format="%H")
curl -o ${TIFLASH_PROXY_BIN_PATH} \
  http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}-llvm/libtiflash_proxy.so
proxy_size=$(ls -l "${TIFLASH_PROXY_BIN_PATH}" | awk '{print $5}')
min_size=$((102400))

if [[ ${proxy_size} -lt ${min_size} ]]; then
  rm -rf ${TIFLASH_PROXY_BIN_PATH}
  echo "${proxy_git_hash}" >/tmp/build_tiflash_proxy_flag # use it as flag to upload binary
else
  rm -rf /tmp/build_tiflash_proxy_flag
  chmod 0731 "${TIFLASH_PROXY_BIN_PATH}"
fi
