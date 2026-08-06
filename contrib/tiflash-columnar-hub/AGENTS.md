# TiFlash Columnar Hub Guide

This directory is the Rust FFI proxy for **next-gen columnar-only** TiFlash (`ENABLE_NEXT_GEN` + `ENABLE_NEXT_GEN_COLUMNAR`). It replaces `contrib/tiflash-proxy` / `contrib/tiflash-proxy-next-gen` in that build mode.

## Role in TiFlash

When CMake enables columnar next-gen, TiFlash links against a shared library built here:

| Artifact | Description |
|----------|-------------|
| `libtiflash_proxy.so` | Rust `cdylib` (`hub-runtime`, crate name `tiflash_proxy`) |
| FFI headers | Under `hub-runtime/ffi/src/` (included as `TIFLASH_PROXY_INCLUDE_DIR`) |

CMake builds it via `contrib/tiflash-proxy-cmake/CMakeLists.txt` (`make release` or `make debug` in this directory). The resulting library is installed next to the `tiflash` binary (for example `tests/.build/tiflash/libtiflash_proxy.so`).

TiFlash C++ talks to this library through the same proxy FFI surface used elsewhere (`run_raftstore_proxy_ffi`, columnar reader APIs, and so on). In columnar-only mode the runtime is much thinner than classic `tiflash-proxy`: there is no full raftstore learner stack in this repo.

## Directory layout

```
contrib/tiflash-columnar-hub/
├── Cargo.toml              # workspace; pins cloud-storage-engine git deps
├── Cargo.lock
├── Makefile                # `make release` / `make debug` entry for CMake
├── rust-toolchain.toml     # nightly toolchain for this workspace
├── workspace-hack/         # local workspace-hack override (avoid jemalloc in cdylib)
└── hub-runtime/            # main crate, builds libtiflash_proxy.so
    ├── Cargo.toml
    ├── ffi/src/            # C ABI / generated FFI bindings for TiFlash C++
    └── src/
        ├── run.rs           # proxy process startup, config, service loop
        ├── hub.rs           # ColumnarHub runtime host
        ├── cloud_helper.rs  # TiKV snapshot / DFS / kvengine SnapAccess integration
        ├── columnar_impls.rs# FFI wrappers around kvengine::CloudColumnarReaders
        ├── domain_impls.rs  # Rust pointer / error helpers for FFI
        ├── basic_ffi_impls.rs
        ├── engine_store_helper.rs
        ├── interfaces.rs
        ├── status_server.rs
        └── profile.rs
```

## What the code does

**Columnar read path (important):** MPP / disaggregated columnar scans in TiFlash C++ eventually call into this library. `cloud_helper.rs` fetches kvengine snapshots from TiKV (`/kvengine/snapshot/...`), and `columnar_impls.rs` drives `kvengine::CloudColumnarReaders` to decode columnar blocks and return them over FFI.

That means **kvengine inside this repo**, not kvengine inside the `tikv-server` binary, owns columnar range decoding and `CloudColumnarReader` behavior on the compute node. Keep proxy and TiKV versions aligned when debugging columnar correctness issues.

**Other responsibilities:**

- Start and host the columnar hub process (`run.rs`)
- Expose proxy status / profiling / metrics hooks expected by TiFlash
- Wire DFS (S3 / builtin), encryption, PD client, and caches through cloud-storage-engine crates

## cloud-storage-engine dependency

All cloud-storage-engine crates are declared once in the workspace root `Cargo.toml` under `[workspace.dependencies]`:

- `kvengine`, `kvenginepb`
- `api_version`, `builtin_dfs`, `cloud_encryption`, `keys`
- `pd_client`, `security`, `tikv_util`

They must share the **same git source and revision**. Do not bump only `kvengine` and leave the others on an older commit.

Upstream repository: `https://github.com/tidbcloud/cloud-storage-engine.git`

### Option A: Pin a git revision (default for CI / reproducible builds)

1. Edit `Cargo.toml` and set the same `rev` on every `[workspace.dependencies]` entry sourced from cloud-storage-engine:

   ```toml
   kvengine = { git = "https://github.com/tidbcloud/cloud-storage-engine.git", rev = "<full-or-short-commit>" }
   ```

2. Refresh the lockfile:

   ```bash
   cd contrib/tiflash-columnar-hub
   cargo update -p kvengine -p kvenginepb -p api_version -p builtin_dfs \
     -p cloud_encryption -p keys -p pd_client -p security -p tikv_util
   ```

3. Sanity-check the build:

   ```bash
   cargo check --manifest-path hub-runtime/Cargo.toml
   # or
   make release
   ```

4. Rebuild TiFlash so the new `libtiflash_proxy.so` is picked up:

   ```bash
   cmake --build <build-dir> --target tiflash
   cmake --install <build-dir> --component=tiflash-release --prefix=tests/.build/tiflash
   ```

5. For integration tests, restart the columnar cluster so `tiflash-cn0` loads the new proxy library.

Verify the resolved revision in `Cargo.lock`:

```bash
rg 'source = "git\+https://github.com/tidbcloud/cloud-storage-engine.git' Cargo.lock | head -3
```

### Option B: Track a branch

Replace `rev = "..."` with `branch = "cloud-engine"` (or another branch) on all workspace entries, then run the same `cargo update` and TiFlash rebuild steps. Branch tracking is convenient for local development but less reproducible than a pinned `rev`.

### Option C: Local path override (fast iteration)

Uncomment the `[patch."https://github.com/tidbcloud/cloud-storage-engine.git"]` block at the bottom of `Cargo.toml` and point crates to a local checkout, for example:

```toml
kvengine = { path = "../../../cloud-storage-engine/components/kvengine" }
```

Adjust paths relative to `contrib/tiflash-columnar-hub/`. Then run `cargo check` / `make release` and rebuild TiFlash. Remember to revert path patches before committing lockfile changes meant for upstream CI.

## Standalone commands

```bash
cd contrib/tiflash-columnar-hub

# Same as CMake release build
make release

# Debug build
make debug

# Direct cargo (output under hub-runtime/target or CARGO_TARGET_DIR from CMake)
cargo build --release --manifest-path hub-runtime/Cargo.toml
```

Optional feature flag when TiFlash uses external jemalloc (CMake sets this automatically for columnar builds):

```bash
make release ENABLE_FEATURES=external-jemalloc
```

## Notes

- `workspace-hack/` intentionally overrides cloud-storage-engine's generated workspace-hack so the cdylib does not export a second jemalloc into the TiFlash process.
- Changing `Cargo.lock` alone is not enough for runtime behavior: always rebuild and redeploy `libtiflash_proxy.so`.
- Columnar integration tests under `tests/fullstack-test-next-gen-columnar/` also use separate TiKV / TiDB binaries from `_env.sh`; upgrading proxy kvengine does not upgrade TiKV. For end-to-end columnar behavior, consider keeping proxy and TiKV cloud-storage-engine commits compatible.
