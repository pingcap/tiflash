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

VERSION="gcc-7.4.0"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

cd ~
mkdir gcc
cd gcc
wget https://mirrors.ustc.edu.cn/gnu/gcc/${VERSION}/${VERSION}.tar.gz
tar xf "${VERSION}.tar.gz"
rm "${VERSION}.tar.gz"

cd ${VERSION}
./contrib/download_prerequisites
mkdir gccbuild
cd gccbuild
../configure --enable-languages=c,c++ --disable-multilib
make -j $THREADS
make install
ln -s /usr/local/bin/gcc /usr/local/bin/cc

yum remove -y gcc

echo "/usr/local/lib64" | tee /etc/ld.so.conf.d/10_local-lib64.conf
ldconfig
gcc --version

rm -rf ~/gcc
