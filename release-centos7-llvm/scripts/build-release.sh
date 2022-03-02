#!/bin/bash

CMAKE_PREFIX_PATH=$1

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

${SCRIPTPATH}/build-tiflash-release.sh "" "${CMAKE_PREFIX_PATH}"
