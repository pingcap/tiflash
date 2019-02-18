#!/bin/bash

mkdir -p /server/build_docker
cd /server/contrib/kvproto
./generate_cpp.sh
cd /server/build_docker
CC=/usr/lib/llvm-5.0/bin/clang CXX=/usr/lib/llvm-5.0/bin/clang++ LLVM_ROOT=/usr/lib/llvm-5.0 cmake /server -DENABLE_EMBEDDED_COMPILER=1 -DENABLE_TESTS=0
make -j $(nproc || grep -c ^processor /proc/cpuinfo) theflash
#ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
