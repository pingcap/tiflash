#!/bin/bash

set -ueo pipefail

DEPS_DIR=${DEPS_DIR:-$(pwd)/deps}
NPROC=${NPROC:-4}
echo "DEPS_DIR=$DEPS_DIR"
echo "NPROC=$NPROC"

function common_install_sys_deps() {
    if [[ $OSTYPE == 'darwin'* ]]; then
        darwin_install_sys_deps
    else
        ubuntu_install_sys_deps
    fi
}
function darwin_install_sys_deps() {
    if [[ ! $OSTYPE == 'darwin'* ]]; then
        return
    fi
    echo "This is a macOS"
    HOMEBREW_NO_AUTO_UPDATE=1 brew install openssl@1.1 autoconf automake
}
function ubuntu_install_sys_deps() {
    grep -i ubuntu /etc/issue
    if [ $? -ne 0 ]; then
        return
    fi
    sudo apt-get install -y \
        git build-essential autoconf libtool pkg-config cmake python3-pip\
        libssl-dev zlib1g-dev libc-ares-dev libreadline-dev ccache
}
function python_install_deps() {
    pip3 install pybind11 pyinstaller dnspython uri requests urllib3 toml setuptools etcd3
}
function common_ensure_dir() {
    if [ ! -d $1 ]; then
        mkdir -p $1
    fi
}
function common_install_rust() {
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
}
function common_install_curl() {
    if [[ $OSTYPE == 'darwin'* ]]; then
        return
    fi
    pushd $DEPS_DIR
    git clone --depth=1 https://github.com/curl/curl.git
    pushd curl
    autoreconf -fi
    ./configure --enable-shared=false --enable-static=true --enable-versioned-symbols \
        --with-openssl --prefix=$(pwd)/install
    make -j $NPROC
    make install
    popd
    popd
}
function common_install_grpc() {
    pushd $DEPS_DIR
    git clone -b v1.26.0 --depth=1 https://github.com/grpc/grpc
    pushd grpc
    git submodule update --init --recursive --depth=1

    common_ensure_dir third_party/cares/cares/build
    pushd third_party/cares/cares/build
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install
    make -j $NPROC
    make install
    popd

    common_ensure_dir third_party/protobuf/dist
    pushd third_party/protobuf/dist
    cmake ../cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install
    make -j $NPROC
    make install
    popd

    common_ensure_dir dist
    pushd dist
    local CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install \
        -DgRPC_CARES_PROVIDER=package -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package \
        -Dc-ares_DIR:PATH=$(pwd)/../third_party/cares/cares/build/install/lib/cmake/c-ares \
        -DProtobuf_INCLUDE_DIR:PATH=$(pwd)/../third_party/protobuf/dist/install/include \
        -DProtobuf_LIBRARY=$(pwd)/../third_party/protobuf/dist/install/lib/libprotobuf.a \
        -DProtobuf_PROTOC_LIBRARY=$(pwd)/../third_party/protobuf/dist/install/lib/libprotoc.a \
        -DProtobuf_PROTOC_EXECUTABLE:FILEPATH=$(pwd)/../third_party/protobuf/dist/install/bin/protoc"

    if [[ $OSTYPE == 'darwin'* ]]; then
        CMAKE_FLAGS+=" -DOPENSSL_ROOT_DIR=`brew --prefix openssl@1.1`"
    fi
    cmake .. $CMAKE_FLAGS
    make -j $NPROC
    make install
    popd

    popd
    popd
}
function configure_tiflash() {
    git submodule update --init --recursive --depth=1
    common_ensure_dir build

    pushd contrib/tiflash-proxy
    make release
    popd
    common_ensure_dir libs/libtiflash-proxy
    cp contrib/tiflash-proxy/target/release/libtiflash_proxy.* libs/libtiflash-proxy/

    pushd cluster_manage
    bash ./release.sh
    common_ensure_dir ../build/dbms/src/Server
    cp -r dist/flash_cluster_manager $(pwd)/../build/dbms/src/Server
    popd

    pushd build
    local CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTS=ON -DNO_WERROR=ON \
    -DProtobuf_INCLUDE_DIR=$DEPS_DIR/grpc/third_party/protobuf/dist/install/include \
    -DProtobuf_LIBRARY=$DEPS_DIR/grpc/third_party/protobuf/dist/install/lib/libprotobuf.a \
    -DProtobuf_PROTOC_EXECUTABLE:FILEPATH=$DEPS_DIR/grpc/third_party/protobuf/dist/install/bin/protoc \
    -DgRPC_DIR:PATH=$DEPS_DIR/grpc/dist/install/lib/cmake/grpc \
    -DGRPC_CPP_PLUGIN:FILEPATH=$DEPS_DIR/grpc/dist/install/bin/grpc_cpp_plugin \
    -Dc-ares_DIR=$DEPS_DIR/grpc/third_party/cares/cares/dist/install/lib/cmake/c-ares \
    -DCMAKE_CXX_FLAGS:STRING=-I$DEPS_DIR/grpc/dist/install/include"
    if [[ ! $OSTYPE == 'darwin'* ]]; then
        CMAKE_FLAGS+="-DCURL_LIBRARY=$DEPS_DIR/curl/install/lib/libcurl.a -DCURL_INCLUDE_DIR=$DEPS_DIR/curl/install/include"
    fi
    cmake .. $CMAKE_FLAGS
}

common_ensure_dir $DEPS_DIR
common_install_sys_deps
python_install_deps
common_install_curl
common_install_rust
common_install_grpc
configure_tiflash
