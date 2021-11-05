#!/bin/bash

set -ueo pipefail

DEPS_DIR=${DEPS_DIR:-$(pwd)/deps}
NPROC=${NPROC:-4}
echo "DEPS_DIR=$DEPS_DIR"
echo "NPROC=$NPROC"

function darwin::install_sys_deps() {
    // TODO: install MacOS system dependencies
}
function ubuntu::install_sys_deps() {
    grep -i ubuntu /etc/issue
    if [ $? -ne 0 ]; then
        return
    fi
    sudo apt-get install -y \
        git build-essential autoconf libtool pkg-config cmake python3-pip\
        libssl-dev zlib1g-dev libc-ares-dev libreadline-dev ccache
}
function python::install_deps() {
    pip3 install pybind11 pyinstaller dnspython uri requests urllib3 toml setuptools etcd3
}
function common::ensure_dir() {
    if [ ! -d $1 ]; then
        mkdir -p $1
    fi
}
function common::install_rust() {
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
}
function common::install_curl() {
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
function common::install_grpc() {
    pushd $DEPS_DIR
#    git clone -b v1.26.0 --depth=1 https://github.com/grpc/grpc
    pushd grpc
    git submodule update --init --recursive --depth=1

    common::ensure_dir third_party/cares/cares/build
    pushd third_party/cares/cares/build
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install
    make -j $NPROC
    make install
    popd

    common::ensure_dir third_party/protobuf/build
    pushd third_party/protobuf/build
    cmake ../cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install
    make -j $NPROC
    make install
    popd

    common::ensure_dir build
    pushd build
    cmake .. -DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX:PATH=$(pwd)/install \
        -DgRPC_CARES_PROVIDER=package -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package \
        -Dc-ares_DIR:PATH=$(pwd)/../third_party/cares/cares/build/install/lib/cmake/c-ares \
        -DProtobuf_INCLUDE_DIR:PATH=$(pwd)/../third_party/protobuf/build/install/include \
        -DProtobuf_LIBRARY=$(pwd)/../third_party/protobuf/build/install/lib/libprotobuf.a \
        -DProtobuf_PROTOC_LIBRARY=$(pwd)/../third_party/protobuf/build/install/lib/libprotoc.a \
        -DProtobuf_PROTOC_EXECUTABLE:FILEPATH=$(pwd)/../third_party/protobuf/build/install/bin/protoc
    make -j $NPROC
    make install
    popd

    popd
    popd
}
function configure_tiflash() {
    git submodule update --init --recursive --depth=1
    common::ensure_dir build

    pushd contrib/tiflash-proxy
#    make release
    popd
    common::ensure_dir libs/libtiflash-proxy
    cp contrib/tiflash-proxy/target/release/libtiflash_proxy.so libs/libtiflash-proxy/

    pushd cluster_manage
    bash ./release.sh
    common::ensure_dir ../build/dbms/src/Server
    cp -r dist/flash_cluster_manager $(pwd)/../build/dbms/src/Server
    popd

    pushd build
    cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTS=ON -DNO_WERROR=ON \
    -DCURL_LIBRARY=$DEPS_DIR/curl/install/lib/libcurl.a \
    -DCURL_INCLUDE_DIR=$DEPS_DIR/curl/install/include \
    -DProtobuf_INCLUDE_DIR=$DEPS_DIR/grpc/third_party/protobuf/build/install/include \
    -DProtobuf_LIBRARY=$DEPS_DIR/grpc/third_party/protobuf/build/install/lib/libprotobuf.a \
    -DProtobuf_PROTOC_EXECUTABLE:FILEPATH=$DEPS_DIR/grpc/third_party/protobuf/build/install/bin/protoc \
    -DgRPC_DIR:PATH=$DEPS_DIR/grpc/build/install/lib/cmake/grpc \
    -DGRPC_CPP_PLUGIN:FILEPATH=$DEPS_DIR/grpc/build/install/bin/grpc_cpp_plugin \
    -Dc-ares_DIR=$DEPS_DIR/grpc/third_party/cares/cares/build/install/lib/cmake/c-ares \
    -DCMAKE_CXX_FLAGS:STRING=-I$DEPS_DIR/grpc/build/install/include
}

common::ensure_dir $DEPS_DIR
#ubuntu::install_sys_deps
#python::install_deps
#common::install_curl
#common::install_rust
common::install_grpc
configure_tiflash
