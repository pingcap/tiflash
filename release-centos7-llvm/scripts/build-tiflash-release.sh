#!/usr/bin/env bash

CMAKE_BUILD_TYPE=$1
CMAKE_PREFIX_PATH=$2

if [[ -z ${CMAKE_BUILD_TYPE} ]]; then
    CMAKE_BUILD_TYPE="RELWITHDEBINFO"
fi

DEFAULT_CMAKE_PREFIX_PATH="/usr/local/"
DEFINE_CMAKE_PREFIX_PATH="-DCMAKE_PREFIX_PATH=${DEFAULT_CMAKE_PREFIX_PATH}"

# https://cmake.org/cmake/help/latest/variable/CMAKE_PREFIX_PATH.html
# CMAKE_PREFIX_PATH should be semicolon-separated list
if [[ ${CMAKE_PREFIX_PATH} ]]; then
    DEFINE_CMAKE_PREFIX_PATH="-DCMAKE_PREFIX_PATH=\"${DEFAULT_CMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH}\""
    echo "CMAKE_PREFIX_PATH is \"${DEFAULT_CMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH}\""
fi

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)

INSTALL_DIR="${SRCPATH}/release-centos7-llvm/tiflash"
rm -rf ${INSTALL_DIR} && mkdir -p ${INSTALL_DIR}

BUILD_DIR="${SRCPATH}/release-centos7-llvm/build-release"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

cmake "${SRCPATH}" ${DEFINE_CMAKE_PREFIX_PATH} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DENABLE_TESTING=OFF \
      -DENABLE_TESTS=OFF \
      -Wno-dev \
      -DUSE_CCACHE=OFF \
      -DRUN_HAVE_STD_REGEX=0 \
      -GNinja

cmake --build . --target tiflash --parallel
cmake --install . --component=tiflash-release --prefix="${INSTALL_DIR}"

# unset LD_LIBRARY_PATH before test
unset LD_LIBRARY_PATH
readelf -d "${INSTALL_DIR}/tiflash"
ldd "${INSTALL_DIR}/tiflash"

