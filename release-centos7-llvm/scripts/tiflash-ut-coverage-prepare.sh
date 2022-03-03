#!/bin/bash

mkdir -p /build

command -v ccache >/dev/null 2>&1
ccache_major=$(ccache --version | head - -n 1 | tr '.' ' ' | awk -e '{ print $3 }')
if [[ $? != 0 || $ccache_major -lt 4 ]]; then
  echo "try to install ccache"
  curl -o /usr/local/bin/ccache http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/ccache-4.5.1
  chmod +x /usr/local/bin/ccache
else
  echo "ccache has been installed"
fi

command -v lcov >/dev/null 2>&1
if [[ $? != 0 ]]; then
  echo "try to install lcov"
  pushd /tmp
  wget http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/lcov-1.15-1.noarch.rpm
  rpm -i lcov-1.15-1.noarch.rpm
  rm -rf lcov-1.15-1.noarch.rpm
  popd
else
  echo "lcov has been installed"
fi

set -ueox pipefail
BASE_NAME=$(basename $0)
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
