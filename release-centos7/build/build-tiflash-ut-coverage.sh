#!/bin/bash

mkdir -p /build

command -v ccache >/dev/null 2>&1
if [[ $? != 0 ]]; then
  echo "try to install ccache"
  wget http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/ccache.x86_64.rpm
  rpm -Uvh ccache.x86_64.rpm
else
  echo "ccache has been installed"
fi

command -v gcovr >/dev/null 2>&1
if [[ $? != 0 ]]; then
  echo "try to install gcovr"
  curl -o /tmp/gcovr.tar http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/gcovr.tar
  cd /tmp && rm -rf gcovr && tar xf gcovr.tar && rm -rf gcovr.tar
  rm -rf /usr/bin/gcovr && ln -s /tmp/gcovr/gcovr /usr/bin/gcovr
else
  echo "gcovr has been installed"
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

CI_CCACHE_USED_SRCPATH="/build/tics"
export INSTALL_DIR=${INSTALL_DIR:-"/build/release-centos7/tiflash"}

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
  sh ${CI_CCACHE_USED_SRCPATH}/release-centos7/build/${BASE_NAME}
  exit 0
fi

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-gcov.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')

rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

USE_CCACHE=ON
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

rm -rf ${SRCPATH}/libs/libtiflash-proxy
mkdir -p ${SRCPATH}/libs/libtiflash-proxy

cd ${SRCPATH}/contrib/tiflash-proxy
proxy_git_hash=$(git log -1 --format="%H")

while [[ true ]]; do
  curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" \
    http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}/libtiflash_proxy.so
  proxy_size=$(ls -l "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" | awk '{print $5}')
  if [[ ${proxy_size} -lt $((102400)) ]]; then
    echo "fail to get ci build tiflash proxy, sleep 60s"
    sleep 60
  else
    chmod 0731 "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so"
    break
  fi
done

BUILD_DIR="/build/release-centos7/build-release"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}
cmake "${SRCPATH}" \
  -DENABLE_EMBEDDED_COMPILER=FALSE \
  -DENABLE_TESTS=ON \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DUSE_CCACHE=${USE_CCACHE} \
  -DTEST_COVERAGE=ON \
  -DTEST_COVERAGE_XML=ON \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON

make -j ${NPROC} gtests_dbms gtests_libcommon gtests_libdaemon
mv "${BUILD_DIR}/dbms/gtests_dbms" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libcommon/src/tests/gtests_libcommon" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libdaemon/src/tests/gtests_libdaemon" "${INSTALL_DIR}/"

ccache -s

if [[ ${UPDATE_CCACHE} == "true" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
  exit 0
fi
