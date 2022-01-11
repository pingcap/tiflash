#!/bin/bash

set -ueox pipefail
source scl_source enable devtoolset-10
export CC=gcc
export CXX=g++

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

install_dir="$SRCPATH/release-centos7-llvm/tiflash"

cd ${SRCPATH}/cluster_manage
./release.sh
mkdir -p "$install_dir/flash_cluster_manager"
cp -r dist/flash_cluster_manager "$install_dir/flash_cluster_manager"
