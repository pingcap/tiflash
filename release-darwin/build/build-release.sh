#!/bin/bash
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ueox pipefail

SCRIPTPATH="$(
      cd "$(dirname "$0")"
      pwd -P
)"
SRCPATH=${1:-$(
      cd $SCRIPTPATH/../..
      pwd -P
)}
PATH=$PATH:/root/.cargo/bin
NPROC=${NPROC:-$(sysctl -n hw.physicalcpu || grep -c ^processor /proc/cpuinfo)}
CMAKE_BUILD_TYPE="RELWITHDEBINFO"

install_dir="$SRCPATH/release-darwin/tiflash"
if [ -d "$install_dir" ]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi
build_dir="$SRCPATH/release-darwin/build-release"
rm -rf $build_dir && mkdir -p $build_dir && cd $build_dir

# use llvm@17
export PATH="$(brew --prefix)/opt/llvm@17/bin:$PATH"
export CC="$(brew --prefix)/opt/llvm@17/bin/clang"
export CXX="$(brew --prefix)/opt/llvm@17/bin/clang++"

cmake "$SRCPATH" \
      -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -DUSE_INTERNAL_SSL_LIBRARY=ON \
      -Wno-dev \
      -DNO_WERROR=ON

cmake --build . --target tiflash --parallel $NPROC
cmake --install . --component=tiflash-release --prefix="$install_dir"

FILE="$install_dir/tiflash"
otool -L "$FILE"

set +e
echo "show ccache stats"
ccache -s

# show version
${FILE} version
