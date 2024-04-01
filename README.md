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

- CMake 3.21.0+
- Clang 17.0.0+ under Linux or AppleClang 15.0.0+ under MacOS
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
# Clang will be available as /usr/bin/clang++-15
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 15 all

# Install other dependencies
sudo apt install -y cmake ninja-build zlib1g-dev libcurl4-openssl-dev ccache
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
<summary><b>CentOS 7</b></summary>

Please refer to [release-centos7-llvm/env/prepare-sysroot.sh](./release-centos7-llvm/env/prepare-sysroot.sh)

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
```

If your MacOS is higher or equal to 13.0, it should work out of the box because by default Apple clang is 14.0.0. But if your MacOS is lower than 13.0, you should install llvm clang manually.

```shell
brew install llvm@15

# check llvm version
clang --version # should be 15.0.0 or higher
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
mkdir cmake-build-debug  # The directory name can be customized
cd cmake-build-debug

cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG

ninja tiflash
```

Note: In Linux, usually you need to explicitly specify to use LLVM.

```shell
# In cmake-build-debug directory:
cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG \
  -DCMAKE_C_COMPILER=/usr/bin/clang-14 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-14
```

In MacOS, if you install llvm clang, you need to explicitly specify to use llvm clang.

Add the following lines to your shell environment, e.g. `~/.bash_profile`.
```shell
export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
export CC="/opt/homebrew/opt/llvm/bin/clang"
export CXX="/opt/homebrew/opt/llvm/bin/clang++"
```

Or use `CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` to specify the compiler, like this:
```shell
cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++
```

After building, you can get TiFlash binary in `dbms/src/Server/tiflash` in the `cmake-build-debug` directory.

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
cd cmake-build-debug
cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG
ninja gtests_dbms       # Most TiFlash unit tests
ninja gtests_libdaemon  # Settings related tests
ninja gtests_libcommon
```

Then, to run these unit tests:

```shell
cd cmake-build-debug
./dbms/gtests_dbms
./libs/libdaemon/src/tests/gtests_libdaemon
./libs/libcommon/src/tests/gtests_libcommon
```

More usages are available via `./dbms/gtests_dbms --help`.

## Run Sanitizer Tests

TiFlash supports testing with thread sanitizer and address sanitizer.

To build unit test executables with sanitizer enabled:

```shell
# In the TiFlash repository root:
mkdir cmake-build-sanitizer
cd cmake-build-sanitizer
cmake .. -GNinja -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=ASan # or TSan
ninja gtests_dbms
ninja gtests_libdaemon
ninja gtests_libcommon
```

There are known false positives reported from leak sanitizer (which is included in address sanitizer). To suppress these errors, set the following environment variables before running the executables:

```shell
LSAN_OPTIONS=suppressions=test/sanitize/asan.suppression
```

## Run Integration Tests

1. Build your own TiFlash binary using debug profile:

   ```shell
   cd cmake-build-debug
   cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG
   ninja tiflash
   ```

2. Start a local TiDB cluster with your own TiFlash binary using TiUP:

   ```shell
   cd cmake-build-debug
   tiup playground nightly --tiflash.binpath ./dbms/src/Server/tiflash

   # Or using a more stable cluster version:
   # tiup playground v6.1.0 --tiflash.binpath ./dbms/src/Server/tiflash
   ```

   [TiUP](https://tiup.io) is the TiDB component manager. If you don't have one, you can install it via:

   ```shell
   curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
   ```

   If you are not running the cluster using the default port (for example, you run multiple clusters), make sure that the port and build directory in `tests/_env.sh` are correct.

3. Run integration tests:

   ```shell
   # In the TiFlash repository root:
   cd tests
   ./run-test.sh

   # Or run specific integration test:
   # ./run-test.sh fullstack-test2/ddl
   ```

Note: some integration tests (namely, tests under `delta-merge-test`) requires a standalone TiFlash service without a TiDB cluster, otherwise they will fail. To run these integration tests: TBD

## Run MicroBenchmark Tests

To build micro benchmark tests, you need release profile and tests enabled:

```shell
# In the TiFlash repository root:
mkdir cmake-build-release
cd cmake-build-release
cmake .. -GNinja -DCMAKE_BUILD_TYPE=RELEASE -DENABLE_TESTS=ON
ninja bench_dbms
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

TBD.

## Contributing

Here is the overview of TiFlash architecture [The architecture of TiFlash's distributed storage engine and transaction layer](/docs/design/0000-00-00-architecture-of-distributed-storage-and-transaction.md).

See [TiFlash Development Guide](/docs/DEVELOPMENT.md) and [TiFlash Design documents](/docs/design).

Before submitting a pull request, please resolve clang-tidy errors and use [format-diff.py](format-diff.py) to format source code, otherwise CI build may raise error.

> **NOTE**: It is required to use clang-format 12.0.0+.

```shell
# In the TiFlash repository root:
merge_base=$(git merge-base upstream/master HEAD)
python3 release-centos7-llvm/scripts/run-clang-tidy.py -p cmake-build-debug -j 20 --files `git diff $merge_base --name-only`
# if there are too much errors, you can try to run the script again with `-fix`
python3 format-diff.py --diff_from $merge_base
```

## License

TiFlash is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
