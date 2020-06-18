#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

install_dir="$SRCPATH/release-darwin/tiflash"
if [ -d "$install_dir" ]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi

$SCRIPTPATH/build-tiflash-proxy.sh
$SCRIPTPATH/build-cluster-manager.sh
$SCRIPTPATH/build-tiflash-release.sh
