# TiFlash

## Building TiFlash

### Prerequisites

- CMake 3.13.2+
- clang-format 12.0.0+

### Setup Compiler

- (macOS) Apple Clang 12.0.0
- (CentOS) GCC 7.3.0

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

## Development

To start developing TiFlash, see [TiFlash Development Guide](/docs/DEVELOPMENT.md) and [TiFlash Design documents](/docs/design).

- [The architecture of TiFlash's distributed storage engine and transaction layer](/docs/design/0000-00-00-architecture-of-distributed-storage-and-transaction.md)
