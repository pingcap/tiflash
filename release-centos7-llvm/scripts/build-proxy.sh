#!/usr/bin/env bash

# Tiflash-Proxy Building script
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
# Keep C++ runtime libs to be linked dynamically (ODR safe). 
# Rustc seems resolve static lib on default even when the
# above flag is set to "dylib=c++"

if [ -f /.dockerenv ]; then
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libc++abi.a
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libc++.a
    rm -rf /usr/local/lib/x86_64-unknown-linux-gnu/libunwind.a

    rm -rf /usr/local/lib/aarch64-unknown-linux-gnu/libc++abi.a
    rm -rf /usr/local/lib/aarch64-unknown-linux-gnu/libc++.a
    rm -rf /usr/local/lib/aarch64-unknown-linux-gnu/libunwind.a
    # On Aarch64, rustc emit dependency of gcc_s at a very first place; however, we do want
    # to make sure our lib is using libunwind and compiler-rt
    if [[ $(uname -m) == 'aarch64' ]]; then
        echo '#!/usr/bin/env bash' > /root/tiflash-link 
        echo '/usr/local/bin/clang -Wl,-l:libunwind.so /usr/local/lib/clang/13.0.0/lib/aarch64-unknown-linux-gnu/libclang_rt.builtins.a ${@//*gcc_s*}' >>  /root/tiflash-link
        chmod +x /root/tiflash-link
        sed -i -e 's/\/usr\/local\/bin\/clang/\/root\/tiflash-link/g' $HOME/.cargo/config 
    fi
fi

make release
