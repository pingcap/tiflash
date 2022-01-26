# TiFlash

## Building TiFlash

### Prerequisites

- CMake 3.13.2+
- clang-format 12.0.0+

### Recommended Compiler

- (macOS) Apple Clang 12.0.0
- (Linux) Clang 13.0.0

### Checkout Source Code

```
# WORKSPACE
$ git clone --recursive https://github.com/pingcap/tics.git
```
### TiFlash on MacOS

#### Build tiflash-proxy
```
# WORKSPACE/tics
$ pushd contrib/tiflash-proxy
$ make release
$ popd
$ mkdir -p libs/libtiflash-proxy 
$ cp contrib/tiflash-proxy/target/release/libtiflash_proxy* libs/libtiflash-proxy
```

#### Build TiFlash
```
# WORKSPACE/tics
$ rm -rf build
$ mkdir -p build
$ pushd build
$ cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTS=1 # default build type: RELWITHDEBINFO
$ make tiflash
$ popd
```
Now you will get TiFlash binary under WORKSPACE/tics/build/dbms/src/Server/tiflash.

### TiFlash with LLVM (Linux)

TiFlash compiles in full LLVM environment (libc++/libc++abi/libunwind/compiler-rt) by default. To quickly setup a LLVM environment, you can use TiFlash Development Environment (see `release-centos7-llvm/env`) (for faster access of precompiled package in internal network, you can use [this link](http://fileserver.pingcap.net/download/development/tiflash-env/v1.0.0/tfilash-env-x86_64.tar.xz)).

#### Create TiFlash Env

The development environment can be easily created with following commands (`docker` and `tar xz` are needed):

```
cd release-centos7-llvm/env
make tiflash-env-$(uname -m).tar.xz
```
Then copy and uncompress `tiflash-env-$(uname -m).tar.xz` to a suitable place.

#### Compile TiFlash Proxy

To compile `libtiflash-proxy.so`, you can first enter the development environment with the loader and then simply invoke the makefile to build. For example, the following commands will do the job:
```
cd /path/to/tiflash/env
./loader
cd /path/to/tiflash/proxy/src/dir
make release
```

#### Compile TiFlash

Similarly, you can simply enter the env to compile and run tiflash:
```
cd /path/to/tiflash-env
./loader
cd /your/build/dir
cmake /path/to/tiflash/src/dir -GNinja -DENABLE_TESTS=ON # also build gtest executables
ninja
```

Now you will get TiFlash binary under `WORKSPACE/tics/build/dbms/src/Server/tiflash`.

#### Develop TiFlash
Because all shared libs are shipped with `tiflash-env` and you may not add those libs to your system loader config, you may experience difficulties running executables compiled by `tiflash-env` with an IDE like CLion or VSCode. To make life easier, we provide an option `TIFLASH_ENABLE_LLVM_DEVELOPMENT`, which helps you to setup rpaths automatically so that you can run them without entering the env. To do so, you can use the following commands (or setup your IDE toolchain with the flags):
```
cmake /path/to/tiflash/src/dir \
  -GNinja \
  -DENABLE_TESTS=ON \
  -DTIFLASH_ENABLE_LLVM_DEVELOPMENT=ON \
  -DCMAKE_PREFIX_PATH=/path/to/tiflash-env/sysroot 
```
Then, you can compile and run tifalsh or tests as normal in your IDE.

#### Generate LLVM Coverage Report
To get a coverage report of unit tests, we recommend using the docker image and our scripts.
```
docker run --rm -it -v /path/to/tiflash/src:/build/tiflash hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /bin/bash # or aarch64
cd /build/tiflash/release-centos7-llvm
sh scripts/build-tiflash-ut-coverage.sh
sh scripts/run-ut.sh

# after running complete

llvm-profdata merge -sparse /tiflash/profile/*.profraw -o /tiflash/profile/merged.profdata
llvm-cov export \
    /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon \
    --format=lcov \
    --instr-profile /tiflash/profile/merged.profdata \
    --ignore-filename-regex "/usr/include/.*" \
    --ignore-filename-regex "/usr/local/.*" \
    --ignore-filename-regex "/usr/lib/.*" \
    --ignore-filename-regex ".*/contrib/.*" \
    --ignore-filename-regex ".*/dbms/src/Debug/.*" \
    --ignore-filename-regex ".*/dbms/src/Client/.*" \
    > /tiflash/profile/lcov.info
mkdir -p /build/tiflash/report
genhtml /tiflash/profile/lcov.info -o /build/tiflash/report
```

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
