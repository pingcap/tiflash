#!/bin/bash

ENABLE_CLANG_TIDY_CHECK=${ENABLE_CLANG_TIDY_CHECK:-true}

if [[ "${ENABLE_CLANG_TIDY_CHECK}" == "true" ]]; then
  command -v clang-tidy >/dev/null 2>&1
  if [[ $? != 0 ]]; then
    curl -o "/usr/local/bin/clang-tidy" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/clang-tidy-12
    chmod +x "/usr/local/bin/clang-tidy"
    # http://mirror.centos.org/centos/8-stream/AppStream/x86_64/os/Packages/clang-libs-12.0.0-1.module_el8.5.0+840+21214faf.x86_64.rpm
    curl -o "/tmp/lib64-clang-12-include.tar.gz" http://fileserver.pingcap.net/download/builds/pingcap/tiflash/ci-cache/lib64-clang-12-include.tar.gz
    pushd /tmp
    tar zxf lib64-clang-12-include.tar.gz
    popd
  else
    echo "clang-tidy has been installed"
  fi
  clang-tidy --version
fi

set -ueox pipefail

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
SRCPATH=${1:-$(
  cd $SCRIPTPATH/../..
  pwd -P
)}
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}

if [[ "${ENABLE_CLANG_TIDY_CHECK}" == "true" ]]; then
  BUILD_DIR="${SRCPATH}/release-centos7-llvm/build-release"

  cd ${BUILD_DIR}
  cmake "${SRCPATH}" \
    -DENABLE_EMBEDDED_COMPILER=FALSE \
    -DENABLE_TESTS=0 \
    -DCMAKE_BUILD_TYPE=RELWITHDEBINFO \
    -DUSE_CCACHE=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DLINKER_NAME=lld \
    -DUSE_LIBCXX=ON \
    -DUSE_LLVM_LIBUNWIND=ON \
    -DUSE_LLVM_COMPILER_RT=ON \
    -DTIFLASH_ENABLE_RUNTIME_RPATH=ON \
    -DRUN_HAVE_STD_REGEX=0 \
    -DCMAKE_AR="/usr/local/bin/llvm-ar" \
    -DCMAKE_RANLIB="/usr/local/bin/llvm-ranlib" \
    -GNinja
  python3 ${SRCPATH}/release-centos7-llvm/scripts/run-clang-tidy.py -p ${BUILD_DIR} -j ${NPROC} --files ".*/tics/dbms/*"
  export ENABLE_CLANG_TIDY_CHECK=false
fi
