#!/usr/bin/env bash

set -euo pipefail

NPROC=$(nproc || grep -c ^processor /proc/cpuinfo)

wget https://github.com/curl/curl/releases/download/curl-7_77_0/curl-7.77.0.tar.gz
tar zxf curl-7.77.0.tar.gz
cd curl-7.77.0
./configure --enable-shared=false --with-openssl --enable-static=true
make -j ${NPROC} && make install

