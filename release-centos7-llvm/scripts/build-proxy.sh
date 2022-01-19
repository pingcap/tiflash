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
    # Rustc emits dependency of gcc_s at a very first place; however, we do want
    # make sure our lib is using libunwind and compiler-rt

    echo '#!/usr/bin/env bash' > /root/tiflash-link
    if [[ $(uname -m) == 'aarch64' ]]; then
        echo '/usr/local/bin/clang -Wl,-Bdynamic -l:libunwind.so -l:libc++abi.so -l:libc++.so /usr/local/lib/clang/13.0.0/lib/aarch64-unknown-linux-gnu/libclang_rt.builtins.a ${@//*gcc_s*}' >> /root/tiflash-link
    else
        echo '/usr/local/bin/clang -Wl,-Bdynamic -l:libunwind.so -l:libc++abi.so -l:libc++.so /usr/local/lib/clang/13.0.0/lib/x86_64-unknown-linux-gnu/libclang_rt.builtins.a ${@//*gcc_s*}' >> /root/tiflash-link
    fi
    chmod +x /root/tiflash-link
    sed -i -e 's/\/usr\/local\/bin\/clang/\/root\/tiflash-link/g' $HOME/.cargo/config
fi

make release
