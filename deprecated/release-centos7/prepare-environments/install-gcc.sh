#!/usr/bin/env bash

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
