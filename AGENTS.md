# TiFlash Agent Guide

This document provides essential information for agentic coding tools operating in the TiFlash repository.
It focuses on the fastest safe path to build, test, and navigate the codebase.

## 🚀 Quick Start
- **Configure (preset):** `cmake --preset dev`
- **Build (preset):** `cmake --build --preset dev`
- **Run one test:** `cmake-build-debug/dbms/gtests_dbms --gtest_filter=TestName.*`

## 🛠 Build & Development

TiFlash uses CMake with presets for configuration and building.

### Build Presets
Common presets defined in `CMakePresets.json`:
- `dev`: DEBUG build with tests enabled. (Recommended for development)
- `release`: RELWITHDEBINFO build without tests.
- `asan`: AddressSanitizer build.
- `tsan`: ThreadSanitizer build.
- `benchmarks`: RELEASE build with benchmarks.

### Dependencies & Versions
- **CMake/Ninja/Clang/LLVM/Python/Rust**: Use versions supported by your platform toolchain.
- **Linux vs macOS**: Toolchains live under `release-linux-llvm/` and `release-darwin/` respectively.
- **Submodules/third-party**: Ensure any required submodules are initialized before building.

### Common Commands
- **Configure & Build (Dev):** `cmake --workflow --preset dev`
- **Preset-only (recommended):**
  - Configure: `cmake --preset dev`
  - Build: `cmake --build --preset dev`
- **Manual Build:**
  ```bash
  mkdir cmake-build-debug && cd cmake-build-debug
  cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG -DENABLE_TESTS=ON
  ninja tiflash
  ```
- **Linting & Formatting:**
  - Format diff: `python3 format-diff.py --diff_from $(git merge-base upstream/master HEAD)`  
    (Use `origin/master` or another base if `upstream` is not configured.)
  - Clang-Tidy: `python3 release-linux-llvm/scripts/run-clang-tidy.py -p cmake-build-debug`

## 🧪 Testing

### Unit Tests (Google Test)
Build targets: `gtests_dbms`, `gtests_libdaemon`, `gtests_libcommon`.

- **Run all:** `cmake-build-debug/dbms/gtests_dbms`
- **Run single test:** `cmake-build-debug/dbms/gtests_dbms --gtest_filter=TestName.*`
- **List tests:** `cmake-build-debug/dbms/gtests_dbms --gtest_list_tests`
- **Parallel runner:**
  ```bash
  python3 tests/gtest_10x.py cmake-build-debug/dbms/gtests_dbms
  ```
- **Other targets:**
  - `cmake-build-debug/dbms/gtests_libdaemon`
  - `cmake-build-debug/dbms/gtests_libcommon`

### Integration Tests
See `tests/AGENTS.md` for prerequisites and usage.

### Sanitizers
When running with ASAN/TSAN, use suppression files:
```bash
LSAN_OPTIONS="suppressions=tests/sanitize/asan.suppression" ./dbms/gtests_dbms
TSAN_OPTIONS="suppressions=tests/sanitize/tsan.suppression" ./dbms/gtests_dbms
```

## 🎨 Code Style (C++)

TiFlash follows a style based on Google, enforced by `clang-format` 17.0.0+.

### General
- **Naming:**
  - Classes/Structs: `PascalCase` (e.g., `StorageDeltaMerge`)
  - Methods/Variables: `camelCase` (e.g., `readBlock`, `totalBytes`)
  - Files: `PascalCase` matching class name (e.g., `StorageDeltaMerge.cpp`)
- **Namespaces:** Primary code resides in `namespace DB`.
- **Headers:** Use `#pragma once`. Use relative paths from `dbms/src` (e.g., `#include <Core/Types.h>`).

### Types & Error Handling
- **Types:** Use explicit width types from `dbms/src/Core/Types.h`: `UInt8`, `UInt32`, `Int64`, `Float64`, `String`.
- **Smart Pointers:** Prefer `std::shared_ptr` and `std::unique_ptr`. Use `std::make_shared` and `std::make_unique`.
- **Error Handling:**
  - Use `DB::Exception`.
  - Pattern: `throw Exception("Message", ErrorCodes::SOME_CODE);`
  - Error codes are defined in `dbms/src/Common/ErrorCodes.cpp` and `errors.toml`.
- **Logging:** Use macros like `LOG_INFO(log, "message {}", arg)`. `log` is usually a `DB::LoggerPtr`.

### Modern C++ Practices
- Prefer `auto` for complex iterators/templates.
- Use `std::string_view` for read-only string parameters.
- Use `fmt::format` for string construction.
- Prefer `FmtBuffer` for complex string building in performance-critical paths.

## 🦀 Rust Code
Rust is used in:
- `contrib/tiflash-proxy`: The proxy layer between TiFlash and TiKV.
- `contrib/tiflash-proxy-next-gen`: Disaggregated architecture components.

Follow standard Rust idioms and `cargo fmt`. Use `cargo clippy` for linting.

## 📚 Module-Specific Guides
For more detailed information on specific subsystems, refer to:
- **Storage Engine**: `dbms/src/Storages/AGENTS.md` (DeltaMerge, KVStore, PageStorage)
- **Computation Engine**: `dbms/src/Flash/AGENTS.md` (Planner, MPP, Pipeline)
- **TiDB Integration**: `dbms/src/TiDB/AGENTS.md` (Schema Sync, Decoding, Collation)
- **Testing Utilities**: `dbms/src/TestUtils/AGENTS.md` (Base classes, Mocking, Data generation)

## 📂 Directory Structure
- `dbms/src`: Main TiFlash C++ source code.
- `libs/`: Shared libraries used by TiFlash.
- `tests/`: Integration and unit test utilities.
- `docs/`: Design and development documentation.
- `release-linux-llvm/`: Build scripts and environment configurations for Linux.

## 💡 Debugging Tips
- **LLDB:** Use to debug crashes or hangs.
- **Coredumps:** Ensure coredumps are enabled in your environment.
- **Failpoints:** TiFlash uses failpoints and syncpoints for testing error paths.
  - Search for `FAIL_POINT_TRIGGER_EXCEPTION` or `FAIL_POINT_PAUSE` for failpoints in the code.
  - Search for `SyncPointCtl` or `SYNC_FOR` for syncpoints in the code.
- **Build artifacts:** If `compile_commands.json` is missing, ensure you configured with a preset.

## 📖 References
- `docs/DEVELOPMENT.md`: General engineering practices.
- `docs/design/`: Design documents for major features.
- [TiDB Developer Guide](https://pingcap.github.io/tidb-dev-guide/): General TiDB ecosystem information.
