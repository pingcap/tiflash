#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
PATH=$PATH:/root/.cargo/bin
NPROC=${NPROC:-$(sysctl -n hw.physicalcpu || grep -c ^processor /proc/cpuinfo)}
CMAKE_BUILD_TYPE="RELWITHDEBINFO"

install_dir="$SRCPATH/release-darwin/tiflash"
if [ -d "$install_dir" ]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi
build_dir="$SRCPATH/release-darwin/build-release"
rm -rf $build_dir && mkdir -p $build_dir && cd $build_dir

cmake "$SRCPATH" \
      -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -Wno-dev \
      -DNO_WERROR=ON

cmake --build . --target tiflash --parallel $NPROC
cmake --install . --component=tiflash-release --prefix="$install_dir"

FILE="$install_dir/tiflash"
otool -L "$FILE"

set +e
echo "show ccache stats"
ccache -s
