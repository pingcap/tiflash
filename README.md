# TiFlash

## Building TiFlash

### Prerequisites

- CMake 3.13.2+
- clang-format 12.0.0+

### Setup Compiler

- (macOS) Apple Clang 12.0.0
- (CentOS) GCC 7.3.0

### Install gRPC Systemwise(skip if already have protoc 3.8.x and gRPC 1.26.0 installed)

**You'd better remove any other `protoc` installation except 3.8.x to get a clean build.**

You should use exact gRPC 1.26.0. Refer to gRPC [document](https://github.com/grpc/grpc/blob/master/BUILDING.md) or our test settings [example](https://github.com/pingcap/kvproto/blob/master/.github/workflows/cpp-test.yaml) for how to do it.

### Checkout Source Code

```
# WORKSPACE
$ git clone --recursive https://github.com/pingcap/tics.git
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
$ cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTS=1 # default build type: RELWITHDEBINFO
$ make tiflash
$ popd
```

Now you will get TiFlash binary under `WORKSPACE/tics/build/dbms/src/Server/tiflash`.

### Notice

Before submitting pull request, please use [format-diff.py](format-diff.py) to format source code, otherwise ci-build may raise error.
```
# WORKSPACE/tics
$ python3 format-diff.py --diff_from `git merge-base ${TARGET_REMOTE_BRANCH} HEAD`
```

You can download the `clang-format` from [muttleyxd/clang-tools-static-binaries](https://github.com/muttleyxd/clang-tools-static-binaries/releases). clang-format 12.0.0+ is required.
