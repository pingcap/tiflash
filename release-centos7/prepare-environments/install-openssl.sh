#!/usr/bin/env bash

set -euo pipefail

wget https://www.openssl.org/source/openssl-1.1.1k.tar.gz
tar xvf openssl-1.1.1k.tar.gz
cd openssl-1.1.1k
./config -fPIC no-shared no-afalgeng --prefix=/usr/local/opt/openssl --openssldir=/usr/local/opt/openssl -static
NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)}
make -j ${NPROC}
make install
