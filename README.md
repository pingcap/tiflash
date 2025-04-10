# TiFlash

![tiflash-architecture](tiflash-architecture.png)

[TiFlash](https://docs.pingcap.com/tidb/stable/tiflash-overview) is a columnar storage component of [TiDB](https://docs.pingcap.com/tidb/stable) and [TiDB Cloud](https://en.pingcap.com/tidb-cloud/), the fully-managed service of TiDB. It mainly plays the role of Analytical Processing (AP) in the Hybrid Transactional/Analytical Processing (HTAP) architecture of TiDB.

TiFlash stores data in columnar format and synchronizes data updates in real-time from [TiKV](https://github.com/tikv/tikv) by Raft logs with sub-second latency. Reads in TiFlash are guaranteed transactionally consistent with Snapshot Isolation level. TiFlash utilizes Massively Parallel Processing (MPP) computing architecture to accelerate the analytical workloads.

TiFlash repository is based on [ClickHouse](https://github.com/ClickHouse/ClickHouse). We appreciate the excellent work of the ClickHouse team.

## Quick Start

### Start with TiDB Cloud

Quickly explore TiFlash with [a free trial of TiDB Cloud](https://tidbcloud.com/free-trial).

See [TiDB Cloud Quick Start Guide](https://docs.pingcap.com/tidbcloud/tidb-cloud-quickstart).

### Start with TiDB

See [Quick Start with HTAP](https://docs.pingcap.com/tidb/stable/quick-start-with-htap) and [Use TiFlash](https://docs.pingcap.com/tidb/stable/use-tiflash).

## Build TiFlash

TiFlash can be built on the following hardware architectures:

- x86-64 / amd64
- aarch64

And the following operating systems:

- Linux
- MacOS

### 1. Prepare Prerequisites

The following packages are required:

- CMake 3.23.0+
- Clang 17.0.0+
- Rust
- Python 3.0+
- Ninja-Build or GNU Make
- Ccache (not necessary but highly recommended to reduce rebuild time)

Detailed steps for each platform are listed below.

<details>
<summary><b>Ubuntu / Debian</b></summary>

```shell
sudo apt update

# Install Rust toolchain, see https://rustup.rs for details
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain none
source $HOME/.cargo/env

# Install LLVM, see https://apt.llvm.org for details
# Clang will be available as /usr/bin/clang++-17
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 17 all

# Install other dependencies
sudo apt install -y cmake ninja-build zlib1g-dev libcurl4-openssl-dev ccache
```

Then, expose the LLVM17 toolchain as the default one in order to use it later in compile:

```shell
export CC="/usr/bin/clang-17"
export CXX="/usr/bin/clang++-17"
```

**Note for Ubuntu 18.04 and Ubuntu 20.04:**

The default installed cmake may be not recent enough. You can install a newer cmake from the [Kitware APT Repository](https://apt.kitware.com):

```shell
sudo apt install -y software-properties-common lsb-release
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
sudo apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"
sudo apt update
sudo apt install -y cmake
```

**If you are facing "ld.lld: error: duplicate symbol: ssl3_cbc_digest_record":**

It is likely because you have a pre-installed libssl3 where TiFlash prefers libssl1. TiFlash has vendored libssl1, so that you can simply remove the one in the system to make compiling work:

```shell
sudo apt remove libssl-dev
```

If this doesn't work, please [file an issue](https://github.com/pingcap/tiflash/issues/new?assignees=&labels=type%2Fquestion&template=general-question.md).

</details>

<details>
<summary><b>Archlinux</b></summary>

```shell
# Install Rust toolchain, see https://rustup.rs for details
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain none
source $HOME/.cargo/env

# Install compilers and dependencies
sudo pacman -S clang lld libc++ libc++abi compiler-rt openmp lcov cmake ninja curl openssl zlib llvm ccache
```

</details>

<details>
<summary><b>Rocky Linux 8</b></summary>

Please refer to [release-linux-llvm/env/prepare-sysroot.sh](./release-linux-llvm/env/prepare-sysroot.sh)

</details>

<details>
<summary><b>MacOS</b></summary>

```shell
# Install Rust toolchain, see https://rustup.rs for details
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain none
source $HOME/.cargo/env

# Install compilers
xcode-select --install

# Install other dependencies
brew install ninja cmake openssl@1.1 ccache

brew install llvm@17
```

Then, expose the LLVM17 toolchain as the default one in order to use it later in compile:

```shell
export PATH="$(brew --prefix)/opt/llvm@17/bin:$PATH"
export CC="$(brew --prefix)/opt/llvm@17/bin/clang"
export CXX="$(brew --prefix)/opt/llvm@17/bin/clang++"
```

</details>

### 2. Checkout Source Code

```shell
git clone https://github.com/pingcap/tiflash.git --recursive -j 20
cd tiflash
```

### 3. Build

To build TiFlash for development:

```shell
# In the TiFlash repository root:
cmake --workflow --preset dev
```

Or if you don't like the preset:

```shell
# In the TiFlash repository root:
mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG
ninja tiflash
```

### Build Options

TiFlash has several CMake build options to tweak for development purposes. These options SHOULD NOT be changed for production usage, as they may introduce unexpected build errors and unpredictable runtime behaviors.

To tweak options, pass one or multiple `-D...=...` args when invoking CMake, for example:

```shell
cd cmake-build-debug
cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG -DFOO=BAR
                                          ^^^^^^^^^
```

- **Build Type**:

  - `-DCMAKE_BUILD_TYPE=RELWITHDEBINFO`: Release build with debug info (default)

  - `-DCMAKE_BUILD_TYPE=DEBUG`: Debug build

  - `-DCMAKE_BUILD_TYPE=RELEASE`: Release build

  Usually you may want to use different build directories for different build types, e.g. a new build directory named `cmake-build-release` for the release build, so that compile unit cache will not be invalidated when you switch between different build types.

- **Build with Unit Tests**:

  - `-DENABLE_TESTS=ON`: Enable unit tests (enabled by default in debug profile)

  - `-DENABLE_TESTS=OFF`: Disable unit tests (default in release profile)

- **Build using GNU Make instead of ninja-build**:

  <details>
  <summary>Click to expand instructions</summary>

  To use GNU Make, simply don't pass `-GNinja` to cmake:

  ```shell
  cd cmake-build-debug
  cmake .. -DCMAKE_BUILD_TYPE=DEBUG
  make tiflash -j
  ```

  > **NOTE**: Option `-j` (defaults to your system CPU core count, otherwise you can optionally specify a number) is used to control the build parallelism. Higher parallelism consumes more memory. If you encounter compiler OOM or hang, try to lower the parallelism by specifying a reasonable number, e.g., half of your system CPU core count or even smaller, after `-j`, depending on the available memory in your system.

  </details>

- **Build with System Libraries**:

  <details>
  <summary>Click to expand instructions</summary>

  For local development, it is sometimes handy to use pre-installed third-party libraries in the system, rather than to compile them from sources of the bundled (internal) submodules.

  Options are supplied to control whether to use internal third-party libraries (bundled in TiFlash) or to try using the pre-installed system ones.

  > **WARNING**: It is NOT guaranteed that TiFlash would still build if any of the system libraries are used.
  > Build errors are very likely to happen, almost all the time.

  You can view these options along with their descriptions by running:

  ```shell
  cd cmake-build-debug
  cmake -LH | grep "USE_INTERNAL" -A3
  ```

  All of these options are default as `ON`, as the names tell, using the internal libraries and build from sources.

  There is another option to append extra paths for CMake to find system libraries:

  - `PREBUILT_LIBS_ROOT`: Default as empty, can be specified with multiple values, separated by `;`

  </details>

- **Build for AMD64 Architecture**:

  <details>
  <summary>Click to expand instructions</summary>

  To deploy TiFlash under the Linux AMD64 architecture, the CPU must support the `AVX2` instruction set. Ensure that `cat /proc/cpuinfo | grep avx2` has output.

  If need to build TiFlash for AMD64 architecture without such instruction set, please use cmake option `-DNO_AVX_OR_HIGHER=ON`.

  </details>

## Run Unit Tests

Unit tests are automatically enabled in debug profile. To build these unit tests:

```shell
# In the TiFlash repository root:
cmake --workflow --preset unit-tests-all
```

Then, to run these unit tests:

```shell
cmake-build-debug/dbms/gtests_dbms
cmake-build-debug/libs/libdaemon/src/tests/gtests_libdaemon
cmake-build-debug/libs/libcommon/src/tests/gtests_libcommon
```

Unit tests take time, because tests are run one by one. You can use our parallel test runner instead:

```shell
python3 tests/gtest_10x.py \
  cmake-build-debug/dbms/gtests_dbms \
  cmake-build-debug/libs/libdaemon/src/tests/gtests_libdaemon \
  cmake-build-debug/libs/libcommon/src/tests/gtests_libcommon
```

More usages are available via `./dbms/gtests_dbms --help`.

## Run Sanitizer Tests

TiFlash supports testing with thread sanitizer and address sanitizer.

To build unit test executables with sanitizer enabled:

```shell
# In the TiFlash repository root:
cmake --workflow --preset asan-tests-all # or tsan-tests-all
```

There are known false positives reported from leak sanitizer (which is included in address sanitizer). To suppress these errors, set the following environment variables before running the executables:

```shell
LSAN_OPTIONS="suppressions=../tests/sanitize/asan.suppression" ./dbms/gtests_dbms ...
# or
TSAN_OPTIONS="suppressions=../tests/sanitize/tsan.suppression" ./dbms/gtests_dbms ...
```

## Run Integration Tests

Check out the [Integration Test Guide](/tests/README.md) for more details.

## Run MicroBenchmark Tests

To build micro benchmark tests, you need release profile and tests enabled:

```shell
# In the TiFlash repository root:
cmake --workflow --preset benchmarks
```

Then, to run these micro benchmarks:

```shell
cd cmake-build-release
./dbms/bench_dbms

# Or run with filter:
# ./dbms/bench_dbms --benchmark_filter=xxx
```

More usages are available via `./dbms/bench_dbms --help`.

## Generate LLVM Coverage Report

To build coverage report, run the script under `release-linux-llvm`

```shell
cd release-linux-llvm
./gen_coverage.sh
# Or run with filter:
# FILTER='*DMFile*:*DeltaMerge*:*Segment*' ./gen_coverage.sh

# After the script finished, it will output the directory of code coverage report, you can check out the files by webbrowser
python3 -m http.server --directory "${REPORT_DIR}" "${REPORT_HTTP_PORT}"
```

## Contributing

Here is the overview of TiFlash architecture [The architecture of TiFlash's distributed storage engine and transaction layer](/docs/design/0000-00-00-architecture-of-distributed-storage-and-transaction.md).

See [TiFlash Development Guide](/docs/DEVELOPMENT.md) and [TiFlash Design documents](/docs/design).

Before submitting a pull request, please resolve clang-tidy errors and use [format-diff.py](format-diff.py) to format source code, otherwise CI build may raise error.

> **NOTE**: It is required to use clang-format 17.0.0+.

```shell
# In the TiFlash repository root:
merge_base=$(git merge-base upstream/master HEAD)
python3 release-linux-llvm/scripts/run-clang-tidy.py -p cmake-build-debug -j 20 --files `git diff $merge_base --name-only`
# if there are too much errors, you can try to run the script again with `-fix`
python3 format-diff.py --diff_from $merge_base
```

## License

TiFlash is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
