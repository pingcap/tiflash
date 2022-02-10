#!/bin/bash

CMAKE_PREFIX_PATH=$1

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)

install_dir="$SRCPATH/release-centos7-llvm/tiflash"
if [[ -d "$install_dir" ]]; then rm -rf "${install_dir:?}"/*; else mkdir -p "$install_dir"; fi

# TODO: Calling this script won't be necessary when we move things within `build-proxy.sh` into tiflash proxy's own build script.
${SCRIPTPATH}/build-tiflash-release.sh "" "${CMAKE_PREFIX_PATH}"
