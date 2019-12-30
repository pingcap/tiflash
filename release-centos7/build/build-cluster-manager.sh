#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

install_dir="$SRCPATH/release-centos7/tiflash"

cd ${SRCPATH}/cluster_manage
./release.sh
cp -r dist/flash_cluster_manager "$install_dir/flash_cluster_manager"
