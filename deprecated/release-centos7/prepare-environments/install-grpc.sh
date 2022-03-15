#!/usr/bin/env bash
# Copyright 2022 PingCAP, Ltd.
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


set -e

VERSION="v1.26.0"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPTPATH}/util.sh
is_arm=$(check_arm_arch)
CMAKE_FLAGS_ADD=""

cd ~
git clone https://github.com/grpc/grpc.git -b ${VERSION}
cd grpc
git submodule update --init

if [[ ${is_arm} == 1 ]]; then
    CMAKE_FLAGS_ADD="-DRUN_HAVE_STD_REGEX=0 -DRUN_HAVE_POSIX_REGEX=0 -DCOMPILE_HAVE_GNU_POSIX_REGEX=0"
    cd third_party/boringssl
    git apply ${SCRIPTPATH}/grpc-arm-compile.patch
fi

cd ~/grpc
mkdir .build
cd .build
cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release ${CMAKE_FLAGS_ADD}
make -j ${THREADS}
make install


cd ~/grpc
rm -rf .build
mkdir .build
cd .build
cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release ${CMAKE_FLAGS_ADD}
make -j ${THREADS}
make install

protoc --version

rm -rf ~/grpc
