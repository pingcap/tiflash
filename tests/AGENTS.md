# TiFlash Tests Guide

This document describes how to run tests under `tests/`.

## Scope
- Integration tests under `tests/fullstack-test*`.
- Mock tests under `tests/delta-merge-test`.
- Next-gen integration tests under `tests/fullstack-test-next-gen`.
- Next-gen columnar integration tests under `tests/fullstack-test-next-gen-columnar`.
- Scripts that pull TiDB next-gen images or binaries under `tests/docker/next-gen-utils/README.md`
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

**Configure local component binaries (optional):** edit `_env.sh`:
  ```bash
  export LOCAL_TiFLASH_BIN_DIR="${LOCAL_TiFLASH_BIN_DIR:-/path/to/tiflash/install/dir}"
  export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-/path/to/cloud-storage-engine/target/release}"
  export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-/path/to/tidb/bin}"
  export LOCAL_PD_BIN_DIR="${LOCAL_PD_BIN_DIR:-/path/to/pd/bin}"   # optional
  ```
  `LOCAL_TiFLASH_BIN_DIR` must be a full install tree (`tiflash`, `libtiflash_proxy.so`, runtime libs), for example from `cmake --install . --component=tiflash-release --prefix <dir>`. When set, `compose.sh` mounts it via `tests/docker/override-yaml/local_tiflash.yaml`.

**Start the cluster manually:**
  ```bash
  cd tests/fullstack-test-next-gen
  ./compose.sh up -d
  ./compose.sh down
  ```

### Fullstack Next-Gen Columnar Tests
- Directory: `tests/fullstack-test-next-gen-columnar`.
- Use this suite for next-gen **columnar-only** topology: `tiflash-cn0` only (no `tiflash-wn0`), with configs under `tests/docker/next-gen-columnar-yaml/` and `tests/docker/next-gen-columnar-config/`.
- Build TiFlash first (for example `cmake --workflow --preset dev` with `-DENABLE_NEXT_GEN=ON`), then set `LOCAL_TiFLASH_BIN_DIR` in `_env.sh` or install to `tests/.build/tiflash`.

**Configure local component binaries (optional):** edit `_env.sh`:
  ```bash
  export LOCAL_TiFLASH_BIN_DIR="${LOCAL_TiFLASH_BIN_DIR:-/path/to/tiflash/install/dir}"
  export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-/path/to/cloud-storage-engine/target/release}"
  export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-/path/to/tidb/bin}"
  export LOCAL_PD_BIN_DIR="${LOCAL_PD_BIN_DIR:-/path/to/pd/bin}"   # optional
  ```
  When set, `compose.sh` mounts these binaries via `tests/docker/override-yaml/`.

#### Run the scripted suite
Brings cluster up, runs tests, and tears down:
  ```bash
  cd tests/fullstack-test-next-gen-columnar
  ENABLE_NEXT_GEN=true ./run.sh
  ```

#### Run the test manually
Start or stop the cluster manually:
  ```bash
  cd tests/fullstack-test-next-gen-columnar
  ./compose.sh up -d
  ./compose.sh ps
  ./compose.sh exec tiflash-cn0 bash
  ./compose.sh down
  ```

**Run one test against a running cluster:**
  ```bash
  cd tests/fullstack-test-next-gen-columnar
  ./compose.sh exec -T tiflash-cn0 bash -c \
    'cd /tests && ENABLE_NEXT_GEN=true verbose=true ./run-test.sh <test-path>'
  ```

**Notes:**
- `_env.sh` sets `NEXT_GEN_COLUMNAR_ONLY=true`, so compose helpers create/wait only columnar paths (`prepare_next_gen_columnar_data_dirs`, `wait_next_gen_columnar_env`).
- Most tests should be executed inside `tiflash-cn0` because failpoints and columnar reads run on the compute node.
- Image tags and registry defaults are controlled by `_env.sh` (`HUB_ADDR`, `PD_BRANCH`, `TIKV_BRANCH`, `TIDB_BRANCH`).

## Notes
- Some tests assume local binaries or shared libraries built from this repository.
- If ports or topology differ, override with environment variables documented in the test directory.
