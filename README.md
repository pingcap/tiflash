# TiFlash

## Building TiFlash

### Prerequisites

- CMake 3.13.2+

### Setup Compiler

- (macOS) Apple Clang 12.0.0
- (CentOS) GCC 7.3.0

### Install gRPC Systemwise

You should use exact gRPC 1.26.0. Refer to gRPC [document](https://github.com/grpc/grpc/blob/master/BUILDING.md) or our test settings [example](https://github.com/pingcap/kvproto/blob/master/.github/workflows/cpp-test.yaml) for how to do it.

### Checkout Source Code

```
# WORKSPACE
$ git clone --recursive https://github.com/pingcap/tics.git
```

### Generate Protos

```
# WORKSPACE/tics
$ pushd contrib/kvproto
$ ./scripts/generate_cpp.sh
$ popd

$ pushd contrib/tipb
$ ./generate-cpp.sh
$ popd
```

### Build tiflash-proxy

```
# WORKSPACE/tics
$ pushd contrib/tiflash-proxy
$ make release
$ popd
$ mkdir -p libs/libtiflash-proxy 
$ cp contrib/tiflash-proxy/target/release/libtiflash_proxy* libs/libtiflash-proxy
```

### Build TiFlash

```
# WORKSPACE/tics
$ rm -rf build
$ mkdir -p build
$ pushd build
$ cmake .. -DCMAKE_BUILD_TYPE=Debug # default: RELWITHDEBINFO
$ make tiflash
$ popd
```

Now you will get TiFlash binary under `WORKSPACE/tics/build/dbms/src/Server/tiflash`.
