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
ENABLE_FORMAT_CHECK=${ENABLE_FORMAT_CHECK:-false}

if [[ "${ENABLE_FORMAT_CHECK}" == "true" ]]; then
  python3 ${SRCPATH}/format-diff.py --repo_path "${SRCPATH}" --check_formatted --diff_from $(git merge-base origin/${BUILD_BRANCH} HEAD) --dump_diff_files_to "/tmp/tiflash-diff-files.json"
  export ENABLE_FORMAT_CHECK=false
fi

CI_CCACHE_USED_SRCPATH="/build/tics"
export INSTALL_DIR=${INSTALL_DIR:-"$SRCPATH/release-centos7-llvm/tiflash"}

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  mkdir -p /build && cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
  sh ${CI_CCACHE_USED_SRCPATH}/release-centos7-llvm/scripts/build-tiflash-ci.sh
  exit 0
fi

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
ENABLE_TESTS=${ENABLE_TESTS:-1}
ENABLE_EMBEDDED_COMPILER="FALSE"
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
BUILD_UPDATE_DEBUG_CI_CCACHE=${BUILD_UPDATE_DEBUG_CI_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}-llvm.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')
if [[ "${CMAKE_BUILD_TYPE}" != "Debug" ]]; then
  ENABLE_TESTS=0
fi
# https://cd.pingcap.net/blue/organizations/jenkins/build_tiflash_multi_branch/activity/
# Each time after a new commit merged into target branch, a task about nightly build will be triggered.
# BUILD_UPDATE_DEBUG_CI_CCACHE is set true in order to build and upload ccache.
if [[ "${BUILD_UPDATE_DEBUG_CI_CCACHE}" != "false" ]]; then
  echo "====== begin to build & upload ccache for ci debug build ======"
  UPDATE_CCACHE=true NPROC=${NPROC} BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug BUILD_UPDATE_DEBUG_CI_CCACHE=false sh ${SRCPATH}/release-centos7-llvm/scripts/build-tiflash-ci.sh
  echo "======  finish build & upload ccache for ci debug build  ======"
fi

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
curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" \
  http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}-llvm/libtiflash_proxy.so
proxy_size=$(ls -l "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" | awk '{print $5}')
min_size=$((102400))
if [[ ${proxy_size} -lt ${min_size} ]]; then
  echo "try to build libtiflash_proxy.so"
  export PATH=$PATH:$HOME/.cargo/bin
  bash "${SCRIPTPATH}/build-proxy.sh"
  echo "try to upload libtiflash_proxy.so"
  cd target/release
  curl -F builds/pingcap/tiflash-proxy/${proxy_git_hash}-llvm/libtiflash_proxy.so=@libtiflash_proxy.so http://fileserver.pingcap.net/upload
  curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}-llvm/libtiflash_proxy.so
fi

chmod 0731 "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so"

BUILD_DIR="$SRCPATH/release-centos7-llvm/build-release"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}
cmake "$SRCPATH" \
  -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
  -DENABLE_TESTS=${ENABLE_TESTS} \
  -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
  -DUSE_CCACHE=${USE_CCACHE} \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON \
  -DLINKER_NAME=lld \
  -DUSE_LIBCXX=ON \
  -DUSE_LLVM_LIBUNWIND=OFF \
  -DUSE_LLVM_COMPILER_RT=OFF \
  -DTIFLASH_ENABLE_RUNTIME_RPATH=ON \
  -DCMAKE_PREFIX_PATH="/usr/local" \
  -DCMAKE_AR="/usr/local/bin/llvm-ar" \
  -DRUN_HAVE_STD_REGEX=0 \
  -DCMAKE_RANLIB="/usr/local/bin/llvm-ranlib" \
  -GNinja

ninja tiflash

if [[ "${CMAKE_BUILD_TYPE}" = "Debug" && ${ENABLE_TESTS} -ne 0 ]]; then
  ninja page_ctl
  ninja page_stress_testing
  # build gtest in `release-centos7/build/build-tiflash-ut-coverage.sh`
fi

ccache -s

if [[ ${UPDATE_CCACHE} == "true" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
fi

source ${SCRIPTPATH}/utils/vendor_dependency.sh

# Reduce binary size by compressing.
llvm-objcopy --compress-debug-sections=zlib-gnu "${BUILD_DIR}/dbms/src/Server/tiflash" "${INSTALL_DIR}/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "${INSTALL_DIR}/libtiflash_proxy.so"
vendor_dependency "${INSTALL_DIR}/tiflash" libc++abi.so    "${INSTALL_DIR}/"

unset LD_LIBRARY_PATH
readelf -d "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"
ls -lha "${INSTALL_DIR}"
