#!/usr/bin/env bash

# Create support sysroot for TiFlash
# Copyright PingCAP, Inc

set -ueox pipefail

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"

SRCPATH=$(
    cd ${SCRIPTPATH}/../
    pwd -P
)

cd ${SRCPATH}
mkdir tiflash 
cd tiflash
mkdir lib

# LLVM support libs
cp /usr/local/lib/x86_64-unknown-linux-gnu/libc++.so.1 lib/
cp /usr/local/lib/x86_64-unknown-linux-gnu/libc++abi.so.1 lib/
cp /usr/local/lib/x86_64-unknown-linux-gnu/libunwind.so.1 lib/

# Glibc support libs
cp /usr/lib64/libc.so.6 lib/
cp /usr/lib64/libpthread.so.0 lib/
cp /usr/lib64/libm.so.6 lib/
cp /usr/lib64/libdl.so.2 lib/
cp /usr/lib64/librt.so.1 lib/

# Loader
cp /lib64/ld-linux-x86-64.so.2 lib/
