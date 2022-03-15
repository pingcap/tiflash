#!/usr/bin/env bash

# Prepare basic environment for CI/CD.

function prepare_basic() {
    yum install -y epel-release centos-release-scl
    yum install -y \
         devscripts \
         fakeroot \
         debhelper \
         libtool \
         ncurses-static \
         libtool-ltdl-devel \
         python3-devel \
         bzip2 \
         chrpath \
    yum install -y curl git perl wget cmake3 glibc-static zlib-devel diffutils ninja-build devtoolset-10
    yum -y install 'perl(Data::Dumper)' 
    yum clean all -y
}
