#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

install_dir="$SRCPATH/release-centos7/tiflash"
if [ -d "$install_dir" ]; then rm -rf "$install_dir"/*; else mkdir -p "$install_dir"; fi

$SCRIPTPATH/build-tiflash-proxy.sh
$SCRIPTPATH/build-cluster-manager.sh

rm -f "$install_dir/libtiflash_proxy.so"
rm -rf "$install_dir/flash_cluster_manager"
cp "$SRCPATH/contrib/tiflash-proxy/target/release/libtiflash_proxy.so" "$install_dir/libtiflash_proxy.so"
cp "$SRCPATH/cluster_manage/dist/flash_cluster_manager" "$install_dir/flash_cluster_manager"
