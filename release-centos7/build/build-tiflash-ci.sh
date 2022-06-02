#!/bin/bash

set -x

ARCH=$(uname -p)

command -v ccache >/dev/null 2>&1
if [[ $? != 0 ]]; then
  echo "try to install ccache"
  wget http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/ccache.x86_64.rpm
  rpm -Uvh ccache.x86_64.rpm
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

command -v cmake-3.22 >/dev/null 2>&1
if [[ $? != 0 ]]; then
  curl -o /root/cmake-3.22.tar.gz http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/cmake-3.22.3-linux-${ARCH}.tar.gz
  cd /root
  tar zxf cmake-3.22.tar.gz
  rm -rf /usr/bin/cmake-3.22
  ln -s /root/cmake-3.22.3-linux-${ARCH}/bin/cmake /usr/bin/cmake-3.22
  rm cmake-3.22.tar.gz
fi
cmake-3.22 --version

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
BUILD_BRANCH=${BUILD_BRANCH:-master}
ENABLE_FORMAT_CHECK=${ENABLE_FORMAT_CHECK:-false}

if [[ "${ENABLE_FORMAT_CHECK}" == "true" ]]; then
  cd ${SRCPATH}
  python3 ${SRCPATH}/format-diff.py --repo_path "${SRCPATH}" --check_formatted --diff_from $(git merge-base origin/${BUILD_BRANCH} HEAD) --dump_diff_files_to "/tmp/tiflash-diff-files.json"
  export ENABLE_FORMAT_CHECK=false
fi

CI_CCACHE_USED_SRCPATH="/build/tics"
export INSTALL_DIR=${INSTALL_DIR:-"$SRCPATH/release-centos7/tiflash"}

if [[ ${CI_CCACHE_USED_SRCPATH} != ${SRCPATH} ]]; then
  rm -rf "${CI_CCACHE_USED_SRCPATH}"
  mkdir -p /build && cd /build
  cp -r ${SRCPATH} ${CI_CCACHE_USED_SRCPATH}
  sh ${CI_CCACHE_USED_SRCPATH}/release-centos7/build/build-tiflash-ci.sh
  exit 0
fi

NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
ENABLE_TEST=${ENABLE_TEST:-1}
ENABLE_EMBEDDED_COMPILER="FALSE"
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Debug}
UPDATE_CCACHE=${UPDATE_CCACHE:-false}
BUILD_BRANCH=${BUILD_BRANCH:-master}
BUILD_UPDATE_DEBUG_CI_CCACHE=${BUILD_UPDATE_DEBUG_CI_CCACHE:-false}
CCACHE_REMOTE_TAR="${BUILD_BRANCH}-${CMAKE_BUILD_TYPE}.tar"
CCACHE_REMOTE_TAR=$(echo "${CCACHE_REMOTE_TAR}" | tr 'A-Z' 'a-z')
if [[ "${CMAKE_BUILD_TYPE}" != "Debug" ]]; then
    ENABLE_TEST=0
fi
# https://cd.pingcap.net/blue/organizations/jenkins/build_tiflash_multi_branch/activity/
# Each time after a new commit merged into target branch, a task about nightly build will be triggered.
# BUILD_UPDATE_DEBUG_CI_CCACHE is set true in order to build and upload ccache.
if [[ "${BUILD_UPDATE_DEBUG_CI_CCACHE}" != "false" ]]; then
  echo "====== begin to build & upload ccache for ci debug build ======"
  UPDATE_CCACHE=true NPROC=${NPROC} BUILD_BRANCH=${BUILD_BRANCH} CMAKE_BUILD_TYPE=Debug BUILD_UPDATE_DEBUG_CI_CCACHE=false sh ${SRCPATH}/release-centos7/build/build-tiflash-ci.sh
  echo "======  finish build & upload ccache for ci debug build  ======"
fi

rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

USE_CCACHE=ON
rm -rf "${SRCPATH}/.ccache"
cache_file="${SRCPATH}/ccache.tar"
rm -rf "${cache_file}"
curl -o "${cache_file}" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}
cache_size=`ls -l "${cache_file}" | awk '{print $5}'`
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
fi
ccache -z

pushd ${SRCPATH}/cluster_manage/
rm -rf ./dist
./release.sh
popd

if [ -d "$SRCPATH/contrib/kvproto" ]; then
  cd "$SRCPATH/contrib/kvproto"
  rm -rf cpp/kvproto
  ./scripts/generate_cpp.sh
  cd -
fi

if [ -d "$SRCPATH/contrib/tipb" ]; then
  cd "$SRCPATH/contrib/tipb"
  rm -rf cpp/tipb
  ./generate-cpp.sh
  cd -
fi

if [ -d "$SRCPATH/dbms/src/Storages/DeltaMerge/File/dtpb" ]; then
  cd "$SRCPATH/dbms/src/Storages/DeltaMerge/File/dtpb"
  rm -rf cpp/dtpb
  ./generate_cpp.sh
  cd -
fi

rm -rf ${SRCPATH}/libs/libtiflash-proxy
mkdir -p ${SRCPATH}/libs/libtiflash-proxy

cd ${SRCPATH}/contrib/tiflash-proxy
proxy_git_hash=`git log -1 --format="%H"`
curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" \
http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}/libtiflash_proxy.so
proxy_size=`ls -l "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" | awk '{print $5}'`
min_size=$((102400))
if [[ ${proxy_size} -lt ${min_size} ]]
then
    echo "try to build libtiflash_proxy.so"
    export PATH=$PATH:$HOME/.cargo/bin
    make release
    echo "try to upload libtiflash_proxy.so"
    cd target/release
    curl -F builds/pingcap/tiflash-proxy/${proxy_git_hash}/libtiflash_proxy.so=@libtiflash_proxy.so http://fileserver.pingcap.net/upload
    curl -o "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${proxy_git_hash}/libtiflash_proxy.so
fi

chmod 0731 "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so"

BUILD_DIR="$SRCPATH/release-centos7/build-release"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}
cmake-3.22 -S "$SRCPATH" \
  -DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER \
  -DENABLE_TESTS=${ENABLE_TESTS} \
  -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
  -DUSE_CCACHE=${USE_CCACHE} \
  -DDEBUG_WITHOUT_DEBUG_INFO=ON

make -j $NPROC tiflash

# copy gtest binary under Debug mode
if [[ "${CMAKE_BUILD_TYPE}" = "Debug" && ${ENABLE_TEST} -ne 0 ]]; then
    #ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
    make -j ${NPROC} gtests_dbms gtests_libcommon
    cp -f "$build_dir/dbms/gtests_dbms" "${INSTALL_DIR}/"
    cp -f "$build_dir/libs/libcommon/src/tests/gtests_libcommon" "${INSTALL_DIR}/"
fi

ccache -s

if [[ ${UPDATE_CCACHE} == "true" ]]; then
  cd ${SRCPATH}
  rm -rf ccache.tar
  tar -cf ccache.tar .ccache
  curl -F builds/pingcap/tiflash/ci-cache/${CCACHE_REMOTE_TAR}=@ccache.tar http://fileserver.pingcap.net/upload
fi

# Reduce binary size by compressing.
objcopy --compress-debug-sections=zlib-gnu "$build_dir/dbms/src/Server/tiflash"
cp -r "${SRCPATH}/cluster_manage/dist/flash_cluster_manager" "${INSTALL_DIR}"/flash_cluster_manager
cp -f "$build_dir/dbms/src/Server/tiflash" "${INSTALL_DIR}/tiflash"
cp -f "${SRCPATH}/libs/libtiflash-proxy/libtiflash_proxy.so" "${INSTALL_DIR}/libtiflash_proxy.so"
ldd "${INSTALL_DIR}/tiflash"

cd "${INSTALL_DIR}"
chrpath -d libtiflash_proxy.so "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"
ls -lh "${INSTALL_DIR}"
