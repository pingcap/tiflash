# TiFlash
![tiflash-architecture](tiflash-architecture.png)

[TiFlash](https://docs.pingcap.com/tidb/stable/tiflash-overview) is a columnar storage component of [TiDB](https://docs.pingcap.com/tidb/stable). It mainly plays the role of Analytical Processing (AP) in the Hybrid Transactional/Analytical Processing (HTAP) architecture of TiDB. 

TiFlash stores data in columnar format and synchronizes data updates in real-time from [TiKV](https://github.com/tikv/tikv) by Raft logs with sub-second latency. Reads in TiFlash are guaranteed transactionally consistent with Snapshot Isolation level. TiFlash utilizes Massively Parallel Processing (MPP) computing architecture to accelerate the analytical workloads.

TiFlash repository is based on the early version of [ClickHouse](https://github.com/ClickHouse/ClickHouse/tree/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f). We appreciate the excellent work of ClickHouse team.

## Building TiFlash

### Prerequisites

- CMake 3.13.2+
- clang-format 12.0.0+
- Python 3.0+
- Rust
  - `curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly`
  - `source $HOME/.cargo/env`
- Apple Clang 12.0.0 for MacOS
- Clang 13.0.0 for Linux

### Checkout Source Code

```
# WORKSPACE
$ git clone --recursive https://github.com/pingcap/tics.git
```

### Build Options [Optional]

TiFlash has several build options to tweak the build for different purposes.
**Basically all these options have proper default values so you shouldn't experience troubles if you skip all this paragraph.**

- `CMAKE_BUILD_TYPE`: `DEBUG` / `RELWITHDEBINFO` (default) / `RELEASE`
- `ENABLE_TESTS`: `ON` / `OFF` (default)
- `USE_INTERNAL_GRPC_LIBRARY` / `USE_INTERNAL_PROTOBUF_LIBRARY`: `TRUE` (default) / `FALSE`
  - One may want to use prebuilt/system-wide gRPC or Protobuf, set them to `FALSE` so CMake will try to find prebuilt gRPC and Protobuf instead of build the ones from the submodules.
  - Option `PREBUILT_LIBS_ROOT` can be used to guide CMake's searching for them.
  - Note that though these two options can be set individually, it is recommended to set them both because we won't guarantee the compatibility between gRPC and Protobuf in arbitrary versions.
- `USE_INTERNAL_TIFLASH_PROXY`: `TRUE` (default) / `FALSE`
  - One may want to use external TiFlash proxy, e.g., if he is developing TiFlash proxy (https://github.com/pingcap/tidb-engine-ext) together with TiFlash.
  - Usually need to be combined with `PREBUILT_LIBS_ROOT="<PATH_TO_YOUR_OWN_TIFLASH_PROXY_REPO>..."`.
  - It assumes the `<PATH_TO_YOUR_OWN_TIFLASH_PROXY_REPO>` has the same directory structure as the TiFlash proxy submodule in TiFlash, i.e.:
    - Header files are under directory `<PATH_TO_YOUR_OWN_TIFLASH_PROXY_REPO>/raftstore-proxy/ffi/src`.
    - Built library is under directory `<PATH_TO_YOUR_OWN_TIFLASH_PROXY_REPO>/target/release`.
- `PREBUILT_LIBS_ROOT`: Paths for CMake to search for prebuilt/system-wide libraries
  - Can be specified with multiple values, seperated by `;`.

These options apply to all the following platforms, take them as needed, and at your own risk.

### Build TiFlash on MacOS

```
# WORKSPACE/tics
$ rm -rf build
$ mkdir -p build
$ pushd build
$ cmake ..
$ make tiflash
$ popd
```
Now you will get TiFlash binary under `WORKSPACE/tics/build/dbms/src/Server/tiflash`.

### Build TiFlash on Linux

TiFlash compiles in full LLVM environment (libc++/libc++abi/libunwind/compiler-rt) by default. To quickly setup a LLVM environment, you can use TiFlash Development Environment (see `release-centos7-llvm/env`). Or you can also use system-wise toolchain if you can install `clang/compiler-rt/libc++/libc++abi` (with development headers, version 13+) in your environment. 

#### Option 1: TiFlash Env

> for faster access of precompiled package in internal network, you can use [this link](http://fileserver.pingcap.net/download/development/tiflash-env/v1.0.0/tfilash-env-x86_64.tar.xz)

The development environment can be easily created with following commands (`docker` and `tar xz` are needed):

```
cd release-centos7-llvm/env
make tiflash-env-$(uname -m).tar.xz
```
Then copy and uncompress `tiflash-env-$(uname -m).tar.xz` to a suitable place.

To enter the env (before compiling TiFlash):

```
cd /path/to/tiflash-env
./loader
```

#### Option 2: System-wise Toolchain 

- Debian/Ubuntu users:

  ```bash
  # add LLVM repo key
  wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add - 

  # install LLVM packages
  apt-get install clang-13 lldb-13 lld-13 clang-tools-13 clang-13-doc libclang-common-13-dev libclang-13-dev libclang1-13 clang-format-13 clangd-13 clang-tidy-13 libc++-13-dev libc++abi-13-dev libomp-13-dev llvm-13-dev libfuzzer-13-dev 
  
  # install rust
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

  # install other dependencies
  apt-get install lcov cmake ninja-build libssl-dev zlib1g-dev libcurl4-openssl-dev
  ```

- Archlinux users:

  ```bash
  # install compilers and dependencies
  sudo pacman -S clang libc++ libc++abi compiler-rt openmp lcov cmake ninja curl openssl zlib
  
  # install rust
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ``` 

#### Compile TiFlash

You can now build tiflash using the following commands:

```
cd /your/build/dir
cmake /path/to/tiflash/src/dir -GNinja
ninja
```

Now you will get TiFlash binary under `WORKSPACE/tics/build/dbms/src/Server/tiflash`.

#### IDE Support
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
