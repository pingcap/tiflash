#!/usr/bin/env bash

set -euo pipefail

wget https://www.openssl.org/source/old/1.1.0/openssl-1.1.0f.tar.gz
tar xvf openssl-1.1.0f.tar.gz
cd openssl-1.1.0f
./config -fPIC no-shared no-afalgeng --prefix=/usr/local/opt/openssl --openssldir=/usr/local/opt/openssl -static
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
make -j ${NPROC}
make install
