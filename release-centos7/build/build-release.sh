#!/bin/bash

CMAKE_PREFIX_PATH=$1

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)

install_dir="$SRCPATH/release-centos7/tiflash"
if [[ -d "$install_dir" ]]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi

${SCRIPTPATH}/build-tiflash-proxy.sh
${SCRIPTPATH}/build-cluster-manager.sh
${SCRIPTPATH}/build-tiflash-release.sh "" "${CMAKE_PREFIX_PATH}"
