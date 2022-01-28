#!/usr/bin/env bash

# TiFlash-Proxy Building script
# Copyright PingCAP, Inc
# THIS SCRIPT SHOULD ONLY BE INVOKED IN DOCKER

set -ueox pipefail

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"
SRCPATH=$(
    cd ${SCRIPTPATH}/../..
    pwd -P
)
PATH=$PATH:/root/.cargo/bin

cd ${SRCPATH}/contrib/tiflash-proxy

# rocksdb, grpc build is configured with WERROR
export CFLAGS="-w" 
export CXXFLAGS="-w"

# override cc-rs default STL lib
export CXXSTDLIB="c++"
export CMAKE="/opt/cmake/bin/cmake"

if [ -f /.dockerenv ]; then
    echo '#!/usr/bin/env bash' > /tmp/tiflash-link
    echo '/usr/local/bin/clang -Wl,-Bdynamic -l:libc++abi.so -l:libc++.so $@' >> /tmp/tiflash-link
    chmod +x /tmp/tiflash-link
    export RUSTFLAGS="-C linker=/tmp/tiflash-link"
fi

make release
