#!/bin/bash

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

cd $SRCPATH/cluster_manage
./release.sh
cp -r dist/flash_cluster_manager/ /flash_cluster_manager
