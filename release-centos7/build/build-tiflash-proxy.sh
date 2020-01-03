#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
PATH=$PATH:/root/.cargo/bin

cd ${SRCPATH}/contrib/tiflash-proxy
make release
