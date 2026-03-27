# Repo Notes

## Local Build/Test

- `gtests_dbms` is the DBMS unit-test binary. Use it for targeted local gtest runs during development.
- In a default debug CMake build, the binary is usually generated at `./cmake-build-debug/dbms/gtests_dbms`.
- If your build directory is different, locate the binary from the CMake build output or with `find . -path '*/dbms/gtests_dbms'`.
- To build `gtests_dbms`, run `cmake --build <build_dir> --target gtests_dbms --parallel 16`.
- If `CCACHE_DIR` and `CCACHE_TEMPDIR` are already set in the environment, reuse them for `gtests_dbms` builds so later incremental builds use the same cache.
- If they are not set, point them to writable locations and keep reusing the same `CCACHE_DIR`. Example:
  `env CCACHE_DIR=/path/to/ccache CCACHE_TEMPDIR=/path/to/ccache-tmp cmake --build <build_dir> --target gtests_dbms --parallel 16`
