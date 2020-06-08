#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd ${SCRIPTPATH}/../..; pwd -P)
PATH=$PATH:/root/.cargo/bin

cd ${SRCPATH}/contrib/tiflash-proxy

source ${SCRIPTPATH}/../prepare-environments/util.sh
if [[ $(check_arm_arch) == 1 ]]; then
    export ROCKSDB_SYS_SSE=0
fi

make release
