#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

install_dir="$SRCPATH/release-centos7/tiflash"
if [ -d "$install_dir" ]; then rm -rf "$install_dir"/*; else mkdir -p "$install_dir"; fi

$SCRIPTPATH/build-cluster-manager.sh

rm -rf "$install_dir/flash_cluster_manager"
cp -r "$SRCPATH/cluster_manage/dist/flash_cluster_manager" "$install_dir/flash_cluster_manager"
