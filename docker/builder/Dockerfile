FROM ubuntu:18.04

RUN apt update -y \
 && apt install -y cmake ccache libssl-dev libcrypto++-dev \
        libglib2.0-dev libltdl-dev libicu-dev libmysql++-dev \
        libreadline-dev libmysqlclient-dev unixodbc-dev \
        unixodbc-dev devscripts dupload fakeroot debhelper \
        gcc-7 g++-7 unixodbc-dev devscripts dupload fakeroot debhelper \
        liblld-5.0-dev libclang-5.0-dev liblld-5.0 \
        build-essential autoconf libtool pkg-config \
        libgflags-dev libgtest-dev \
        golang curl python3-pip \
        # For tests: # bash expect python python-lxml python-termcolor curl perl sudo tzdata
 && curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly \
 && pip3 install pybind11 pyinstaller dnspython uri requests urllib3 toml setuptools \
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/grpc/grpc.git && cd grpc && git checkout v1.24.3 && git submodule update --init \
 && cd /grpc && mkdir .build && cd .build && cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo) \
 && rm -rf /grpc/.build \
 && cd /grpc && mkdir .build && cd .build && cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo) \
 && source /root/.profile && ldconfig \
 && cd / && mkdir libtiflash-proxy && git clone -b tiflash-proxy-lib https://github.com/solotzg/tikv.git tiflash-proxy && cd /tiflash-proxy \
 && make release && cp target/release/libtiflash_proxy.so /libtiflash-proxy && cd / && rm -rf /tiflash-proxy \
 && rustup self uninstall -y \
 && git clone https://github.com/pingcap/tics.git /tics-build \
 && cd tics-build && cd cluster_manage \
 && ./release.sh \
 && rm -rf /flash_cluster_manager && cp -r dist/flash_cluster_manager/ /flash_cluster_manager \
 && cd / && rm -rf tics-build/
