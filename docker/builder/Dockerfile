FROM ubuntu:17.10

RUN apt update -y && \
    apt install -y cmake libssl-dev libcrypto++-dev \
        libglib2.0-dev libltdl-dev libicu-dev libmysql++-dev \
        libreadline-dev libmysqlclient-dev unixodbc-dev \
        gcc-7 g++-7 unixodbc-dev devscripts dupload fakeroot debhelper \
        liblld-5.0-dev libclang-5.0-dev liblld-5.0
        # For tests: # bash expect python python-lxml python-termcolor curl perl sudo tzdata

RUN apt install -y build-essential autoconf libtool pkg-config
RUN apt install -y libgflags-dev libgtest-dev
RUN apt install -y clang-5.0 libc++-dev
RUN apt install -y golang libcurl4-gnutls-dev

RUN git clone https://github.com/grpc/grpc.git && cd grpc && git checkout v1.14.2 && git submodule update --init
RUN cd /grpc && mkdir .build && cd .build && CC=clang CXX=clang++ cmake .. -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo)
RUN cd /grpc && rm -rf .build && mkdir .build && cd .build && CC=clang CXX=clang++ cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DCMAKE_BUILD_TYPE=Release && make install -j $(nproc || grep -c ^processor /proc/cpuinfo)

ADD build.sh /
RUN chmod +x /build.sh

CMD ["/build.sh"]
