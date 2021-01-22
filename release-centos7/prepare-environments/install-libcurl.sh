#!/usr/bin/env bash

set -euo pipefail

NPROC=$(nproc || grep -c ^processor /proc/cpuinfo)

wget https://curl.haxx.se/download/curl-7.74.0.tar.gz
tar zxf curl-7.74.0.tar.gz
cd curl-7.74.0
./configure --enable-shared=false --enable-static=true
make -j ${NPROC} && make install

