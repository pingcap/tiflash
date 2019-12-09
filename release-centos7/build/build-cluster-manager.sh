#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}

set -xe

cd $SRCPATH/cluster_manage
./release.sh
cp -r dist/flash_cluster_manager/ /flash_cluster_manager
