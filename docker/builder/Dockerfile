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
        golang \
        # For tests: # bash expect python python-lxml python-termcolor curl perl sudo tzdata
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/grpc/grpc.git && cd grpc && git checkout v1.14.2 && git submodule update --init \
 && cd /grpc && mkdir .build && cd .build && cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo) \
 && rm -rf /grpc/.build \
 && cd /grpc && mkdir .build && cd .build && cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo)
