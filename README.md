# TiFlash
![tiflash-architecture](tiflash-architecture.png)

[TiFlash](https://docs.pingcap.com/tidb/stable/tiflash-overview) is a columnar storage component of [TiDB](https://docs.pingcap.com/tidb/stable). It mainly plays the role of Analytical Processing (AP) in the Hybrid Transactional/Analytical Processing (HTAP) architecture of TiDB. 

TiFlash stores data in columnar format and synchronizes data updates in real-time from [TiKV](https://github.com/tikv/tikv) by Raft logs with sub-second latency. Reads in TiFlash are guaranteed transactionally consistent with Snapshot Isolation level. TiFlash utilizes Massively Parallel Processing (MPP) computing architecture to accelerate the analytical workloads.

TiFlash repository is based on [ClickHouse](https://github.com/ClickHouse/ClickHouse). We appreciate the excellent work of ClickHouse team.

## Build TiFlash

TiFlash supports building on the following hardware architectures:
* x86-64/amd64
* aarch64

And the following operating systems:
* Linux
* MacOS

### Checkout Source Code

Assume `$WORKSPACE` to be the directory under which the TiFlash repo is placed.

```bash
cd $WORKSPACE
git clone --recursive https://github.com/pingcap/tics.git
```

### Prerequisites

The following packages are needed for all platforms:
- CMake 3.13.2+
- Rust
  ```bash
  curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly
  source $HOME/.cargo/env
  ```
- Python 3.0+
- Ninja or GNU Make

The following are platform-specific prerequisites.

#### Linux

TiFlash can be built using either LLVM or GCC toolchain on Linux. LLVM toolchain is our official one for releasing.
> But for GCC, only GCC 7.x is supported as far, and is not planned to be a long term support. So it may get broken some day, silently.

##### LLVM 13.0.0+

TiFlash compiles using full LLVM toolchain (`clang/compiler-rt/libc++/libc++abi`) by default.
To quickly set up a LLVM environment, you can use TiFlash Development Environment (short as TiFlash Env, see `release-centos7-llvm/env`).
Or you can also use system-wise toolchain if you can install `clang/compiler-rt/libc++/libc++abi` in your environment.

###### TiFlash Env

TiFlash Env can be created with the following commands (`docker` and `tar xz` are needed):

```bash
cd $WORKSPACE/tics/release-centos7-llvm/env
make tiflash-env-$(uname -m).tar.xz
```

Then copy and uncompress `tiflash-env-$(uname -m).tar.xz` to a suitable place, assuming `$TIFLASH_ENV`.

To enter the env (before compiling TiFlash):

```bash
cd $TIFLASH_ENV
./loader
```

Or you can dump the env settings and put them to the end of your `~/.bashrc` or `~/.zshrc`

```bash
cd $TIFLASH_ENV
./loader-env-dump
```

###### System-wise Toolchain

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

##### GCC 7.x

> **WARNING**: This support may not be maintained in the future.

TiFlash compiles on GCC 7.x (no older, nor newer) only because it hasn't been broken. If you have GCC 7.x, you are probably fine, for now.

#### MacOS

- Apple Clang 12.0.0+
- OpenSSL 1.1
  ```bash
  brew install openssl@1.1
  ```

### Build

Assume `$BUILD` to be the directory under which you want to build TiFlash.

For Ninja:

```bash
cd $BUILD
cmake $WORKSPACE/tics -Gninja
ninja tiflash
```

For GNU Make:

```bash
cd $BUILD
cmake $WORKSPACE/tics
make tiflash -j
```

After building, you can get TiFlash binary under `$BUILD/dbms/src/Server/tiflash`.

### Build Options

TiFlash has several build options to tweak the build, mostly for development purposes.

> **WARNING**: These options SHOULD NOT be tuned for production usage, as they may introduce unexpected build errors and unpredictable runtime behaviors.

They are all CMake options thus are specified using `-D...=...`s in CMake command line.

#### Build Types

- `CMAKE_BUILD_TYPE`: `DEBUG` / `RELWITHDEBINFO` (default) / `RELEASE`

#### Build with Unit Tests

- `ENABLE_TESTS`: `ON` / `OFF` (default)

#### Build with System Libraries

For local development, it is sometimes handy to use pre-installed third-party libraries in the system, rather than to compile them from sources of the bundled (internal) submodules.

Options are supplied to control whether to use internal third-party libraries (bundled in TiFlash) or to try using the pre-installed system ones.

> **WARNING**: It is NOT guaranteed that TiFlash would still build if any of the system libraries are used.
> Build errors are very likely to happen, almost all the time.

You can view these options along with their descriptions by running:

```bash
cd $BUILD
cmake -LH | grep "USE_INTERNAL" -A3
```

All of these options are default as `ON`, as the names tell, using the internal libraries and build from sources.

There is another option to append extra paths for CMake to find system libraries:

- `PREBUILT_LIBS_ROOT`: Default as empty, can be specified with multiple values, seperated by `;`

Specifically, for [TiFlash proxy](https://github.com/pingcap/tidb-engine-ext):

- `USE_INTERNAL_TIFLASH_PROXY`: `TRUE` (default) / `FALSE`
  - One may want to use external TiFlash proxy, e.g., if he is developing TiFlash proxy together with TiFlash, assume `$TIFLASH_PROXY_REPO` to be the path to the external TiFlash proxy repo
  - Usually need to be combined with `PREBUILT_LIBS_ROOT=$TIFLASH_PROXY_REPO`, and `$TIFLASH_PROXY_REPO` should have the following directory structure:
    - Header files are under directory `$TIFLASH_PROXY_REPO/raftstore-proxy/ffi/src`
    - Built library is under directory `$TIFLASH_PROXY_REPO/target/release`

### IDE Support

Normally a CMake-based IDE, e.g., Clion and VSCode, should be able to open TiFlash project with no pain as long as the toolchains are properly configured.

If your toolchain is set up using [TiFlash Env](#tiflash-env), and you may not want to add those libs to your system loader config, you can pass the following CMake options to your IDE:

```
-DCMAKE_PREFIX_PATH=$TIFLASH_ENV
```

Remember that `$TIFLASH_ENV` is a placeholder mentioned in [TiFlash Env](#tiflash-env).

## Run Unit Tests

TBD.

## Run Integration Tests

TBD.

## Generate LLVM Coverage Report

[//]: <> (TODO: This section is not proper for developers outside PingCAP, as it uses docker image only available on internal network.)
[//]: <> (TODO: Should refine to use local commands rather than docker.)
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

## Contributing

Here is the overview of TiFlash architecture [The architecture of TiFlash's distributed storage engine and transaction layer](/docs/design/0000-00-00-architecture-of-distributed-storage-and-transaction.md).

See [TiFlash Development Guide](/docs/DEVELOPMENT.md) and [TiFlash Design documents](/docs/design).

Before submitting a pull request, please use [format-diff.py](format-diff.py) to format source code, otherwise CI build may raise error.

> **NOTE**: It is required to use clang-format 12.0.0+.

```
cd $WORKSPACE/tics
python3 format-diff.py --diff_from `git merge-base ${TARGET_REMOTE_BRANCH} HEAD`
```
