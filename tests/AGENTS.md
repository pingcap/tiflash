# TiFlash Tests Guide

This document describes how to run tests under `tests/`.

## Scope
- Integration tests under `tests/fullstack-test*`.
- Mock tests under `tests/delta-merge-test`.
- Next-gen integration tests under `tests/fullstack-test-next-gen`.
- Unit test workflows are described in the repository root `AGENTS.md`.

## Build Prerequisite
- Build a TiFlash binary before running tests (for example, `cmake --workflow --preset dev`).

## Test Suites
### Fullstack Integration Tests
- **Prerequisites:** A running TiDB/PD/TiKV cluster with a TiFlash node.
- **Cluster setup:** See `tests/README.md` for `tiup playground` guidance.
- **Run a directory:**
  ```bash
  tidb_port=4000 storage_port=9000 ./run-test.sh <test-dir>
  ```
- `test-dir` is under `tests/fullstack-test*` (for example, `fullstack-test/ddl`).

### Delta-Merge Mock Tests
- **No TiDB cluster needed.** Run a standalone TiFlash server instead.
- **Example:**
  ```bash
  export TIFLASH_PORT=9000
  /path/to/tiflash server -- --path /tmp/tiflash/data --tcp_port ${TIFLASH_PORT}
  storage_port=${TIFLASH_PORT} verbose=true ./run-test.sh delta-merge-test
  ```

### Fullstack Next-Gen Tests
- Requires a disaggregated TiFlash cluster and access to PingCAP internal docker images.
- Build with `-DENABLE_NEXT_GEN=ON`, then run from `tests/fullstack-test-next-gen`:
  ```bash
  ENABLE_NEXT_GEN=true ./run.sh
  ```
- See `tests/README.md` for the full build and docker compose steps.

## Notes
- Some tests assume local binaries or shared libraries built from this repository.
- If ports or topology differ, override with environment variables documented in the test directory.
