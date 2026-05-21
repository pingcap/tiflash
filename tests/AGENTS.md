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

### Fullstack Next-Gen Tests (Columnar Cluster)

Next-gen tests run against a disaggregated columnar cluster managed by `podman compose` / `docker compose`. The cluster uses internal PingCAP images for PD/TiKV/TiDB/MinIO, and mounts a locally built TiFlash binary from `tests/.build/tiflash`.

#### Prerequisites
- `podman compose`, `docker compose`, or `docker-compose`.
- Access to `us-docker.pkg.dev/pingcap-testing-account/tidbx` (or override image vars in `_env.sh`).
- A next-gen TiFlash binary installed to `tests/.build/tiflash/tiflash`.

#### 1. Build TiFlash Columnar for next-gen
```bash
# From repository root
cmake --preset dev -DENABLE_NEXT_GEN=ON -DENABLE_NEXT_GEN_COLUMNAR=ON
cmake --build --preset dev --target tiflash
cmake --install cmake-build-debug --component=tiflash-release --prefix "${PROJECT_DIR}/tests/.build/tiflash"
```

#### 2. Configure local environment
Edit `tests/fullstack-test-next-gen/_env.sh`. Variables use `${VAR:-default}`; command-line exports take precedence.

| Variable | Purpose |
|----------|---------|
| `LOCAL_PD_BIN_DIR` | Directory containing `pd-server`. Empty = use image binary. |
| `LOCAL_TIKV_BIN_DIR` | Directory containing `tikv-server` and `tikv-worker`. Empty = use image binary. |
| `LOCAL_TIDB_BIN_DIR` | Directory containing `tidb-server`. Empty = use image binary. |
| `EXPOSE_TIDB_PORT` | Map `tidb0:4000` to host (for example `4000`). Empty = no host port. |
| `PD_BRANCH`, `TIKV_BRANCH`, `TIDB_BRANCH` | Image tags for PD/TiKV/TiDB. |

Example for local TiKV/TiDB binaries:
```bash
export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-/path/to/tikv/target/release}"
export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-/path/to/tidb/bin}"
# export EXPOSE_TIDB_PORT="${EXPOSE_TIDB_PORT:-4000}"
```

#### 3. Start the next-gen columnar cluster
```bash
cd tests/fullstack-test-next-gen
./compose.sh up -d
./compose.sh ps
```

`./compose.sh` wraps compose with:
- Rocky Linux 9 image selection on RL9 hosts (`disagg_tiflash.rocky9.yaml`).
- Optional local binary override yaml files when `LOCAL_*_BIN_DIR` is set.
- Automatic creation of `data/` and `log/` directories.

Cluster services: `pd0`, `tikv0`, `tikv-worker0`, `tidb0`, `minio0`, `tiflash-cn0`.

TiFlash compute node config lives in `tests/docker/next-gen-config/tiflash_cn.toml` (`use_columnar = true`, `disaggregated_mode = "tiflash_compute"`). The local TiFlash binary is bind-mounted from `tests/.build/tiflash`.

Wait until TiDB and TiFlash are ready (check logs under `log/tidb0/` and `log/tiflash-cn0/` for `server is running MySQL protocol` and `Start to wait for terminal signal`).

#### 4. Run tests

**Single test case** (recommended for local debugging):
```bash
./compose.sh exec -T tiflash-cn0 bash -c \
  'cd /tests && ENABLE_NEXT_GEN=true ./run-test.sh fullstack-test/sample.test'
```

**Full next-gen test suite** (starts cluster, runs all cases, tears down):
```bash
ENABLE_NEXT_GEN=true ./run.sh
verbose=true ENABLE_NEXT_GEN=true ./run.sh
```

**Interactive shell inside TiFlash compute node:**
```bash
./compose.sh exec tiflash-cn0 bash
# inside container:
cd /tests && ENABLE_NEXT_GEN=true ./run-test.sh fullstack-test/sample.test
mysql -h tidb0 -P 4000 -u root -D test
```

#### 5. Stop and clean up
```bash
./compose.sh down
# remove leftover containers from older compose files:
./compose.sh down --remove-orphans
```

#### Troubleshooting
- **`tests/.build/tiflash` missing:** run the build/install step above before `./compose.sh up`.
- **Local binary not picked up:** confirm `LOCAL_*_BIN_DIR` is exported in `_env.sh` or on the command line before `./compose.sh up`.
- logging are available under `tests/fullstack-test-next-gen/log`

See also `tests/README.md` for additional background.

## Notes
- Some tests assume local binaries or shared libraries built from this repository.
- If ports or topology differ, override with environment variables documented in the test directory.
