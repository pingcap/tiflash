#!/usr/bin/env bash

set -e

BRANCH="tags/RELEASE_500/final"
THREADS=$(nproc || grep -c ^processor /proc/cpuinfo)

yum install -y subversion

cd ~
mkdir llvm
cd llvm
svn co "http://llvm.org/svn/llvm-project/llvm/${BRANCH}" llvm

cd llvm/tools
svn co "http://llvm.org/svn/llvm-project/cfe/${BRANCH}" clang
svn co "http://llvm.org/svn/llvm-project/lld/${BRANCH}" lld
svn co "http://llvm.org/svn/llvm-project/polly/${BRANCH}" polly

cd clang/tools
svn co "http://llvm.org/svn/llvm-project/clang-tools-extra/${BRANCH}" extra

cd ../../../..
cd llvm/projects/
svn co "http://llvm.org/svn/llvm-project/compiler-rt/${BRANCH}" compiler-rt
svn co "http://llvm.org/svn/llvm-project/libcxx/${BRANCH}" libcxx
svn co "http://llvm.org/svn/llvm-project/libcxxabi/${BRANCH}" libcxxabi

cd ../..
mkdir build
cd build/
cmake -D CMAKE_BUILD_TYPE:STRING=Release ../llvm
make -j $THREADS
make install

yum remove -y subversion
rm -rf ~/llvm
