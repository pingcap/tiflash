#!/bin/bash

set -ueo pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
PATH=$PATH:/root/.cargo/bin

cd / && mkdir libtiflash-proxy
git clone -b tiflash-proxy-lib https://github.com/solotzg/tikv.git tiflash-proxy
cd /tiflash-proxy && make release
cp target/release/libtiflash_proxy.so /libtiflash-proxy
rm -rf /tiflash-proxy

# rustup self uninstall -y
