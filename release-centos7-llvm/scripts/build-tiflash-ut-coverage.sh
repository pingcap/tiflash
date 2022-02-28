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

CI_CCACHE_USED_SRCPATH="/build/tics"
export INSTALL_DIR=${INSTALL_DIR:-"/build/release-centos7-llvm/tiflash"}

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
  sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/${BASE_NAME}
  exit 0
fi

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-utcov-llvm.tar"
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

BUILD_DIR="/build/release-centos7-llvm/build-release"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}
cmake "${SRCPATH}" \
  -DENABLE_TESTS=ON \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DUSE_CCACHE=${USE_CCACHE} \
  -DTEST_LLVM_COVERAGE=ON \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON \
  -DLINKER_NAME=lld \
  -DUSE_LIBCXX=ON \
  -DUSE_LLVM_LIBUNWIND=OFF \
  -DRUN_HAVE_STD_REGEX=0 \
  -DUSE_LLVM_COMPILER_RT=OFF \
  -DTIFLASH_ENABLE_RUNTIME_RPATH=ON \
  -DCMAKE_PREFIX_PATH="/usr/local" \
  -GNinja

ninja gtests_dbms gtests_libcommon gtests_libdaemon
mv "${BUILD_DIR}/dbms/gtests_dbms" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libcommon/src/tests/gtests_libcommon" "${INSTALL_DIR}/"
mv "${BUILD_DIR}/libs/libdaemon/src/tests/gtests_libdaemon" "${INSTALL_DIR}/"

ccache -s

ls -lh "${INSTALL_DIR}"

if [[ ${UPDATE_CCACHE} == "true" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
  exit 0
fi
