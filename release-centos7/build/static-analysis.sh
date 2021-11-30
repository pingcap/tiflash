#!/bin/bash

function check_src_path() {
  SRC_PATH=$1
  if [[ ! -d "${SRC_PATH}" ]]; then
    echo "no header file in ${SRC_PATH}"
    exit 1
  fi
}

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

# check with clang-tidy after build to generate kvproto & tipb & re2_st.
if [[ "${ENABLE_CLANG_TIDY_CHECK}" == "true" ]]; then
  BUILD_DIR="${SRCPATH}/release-centos7/build-release"
  # DO NOT remove build cache for `re2_st`.
  #rm -rf ${BUILD_DIR}

  check_src_path "${SRCPATH}/contrib/kvproto/cpp/kvproto"
  check_src_path "${SRCPATH}/contrib/tipb/cpp/tipb"
  check_src_path "${BUILD_DIR}/contrib/re2_st/re2_st"

  cd ${BUILD_DIR}
  cmake "${SRCPATH}" \
    -DENABLE_EMBEDDED_COMPILER=FALSE \
    -DENABLE_TESTS=0 \
    -DCMAKE_BUILD_TYPE=RELWITHDEBINFO \
    -DUSE_CCACHE=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  python3 ${SRCPATH}/release-centos7/build/fix_compile_commands.py --file_path=${BUILD_DIR}/compile_commands.json --includes=/tmp/usr/lib64/clang/12.0.0/include --load_diff_files_from "/tmp/tiflash-diff-files.json"
  python3 ${SRCPATH}/release-centos7/build/run-clang-tidy.py -p ${BUILD_DIR} -j ${NPROC} --files ".*/tics/dbms/*"
  ###
  export ENABLE_CLANG_TIDY_CHECK=false
fi
