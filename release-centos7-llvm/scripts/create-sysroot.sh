#!/usr/bin/env bash

# Create support sysroot for TiFlash
# TODO: cmake>=3.21.0 actually provides a series of functions
#       to install dependencies automatically; however, there
#       are some ugly entanglements in our cmakelists. Maybe 
#       we can refactor cmakelists later ¯\_(ツ)_/¯
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
mkdir bin

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

# Extra Dependencies
cp /usr/lib64/libnsl.so.1 lib/

# Loader
cp /lib64/ld-linux-x86-64.so.2 lib/
