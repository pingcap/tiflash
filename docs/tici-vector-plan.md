# TiCI Vector Query Plan

## Current State

- The `feature/fts` TiCI path in TiFlash now supports both FTS mode and vector mode.
- `TypeIndexScan -> TiCIScan -> StorageTantivy -> tici-search-lib::{search, search_vector}(...)` no longer assumes `fts_query_info` only.
- `TypeInverted` and old `ANN` query handling are still on the legacy `TableScan/DeltaMerge` path.
- Current rollout scope is vector-only on the TiCI path; optional pushed-down filter is deferred.
- Local macOS build validation passed on `2026-03-19`; `cmake-build-codex-release/dbms/src/Server/tiflash` was built successfully.
- Runtime smoke/e2e validation is still pending after compile success.

## Target

- Route vector index query through the TiCI path as well.
- Treat the legacy ANN path in TiFlash as non-target for new work.

## Minimal Changes Needed

- Generalize TiFlash `TiCIScan` from FTS-only to a query mode that supports both `FTS` and `Vector`.
- Add a TiFlash-side branch to call `tici-search-lib::search_vector(...)` instead of only `search(...)`.
- Pass vector query parameters through the TiCI pipeline, especially:
  - target `(column_id, distance_metric)`
  - query vector
  - top-k
  - optional filter query later

## Main Code Entry Points

- `dbms/src/Flash/Planner/PhysicalPlan.cpp`
- `dbms/src/Flash/Coprocessor/TiCIScan.cpp`
- `dbms/src/Storages/Tantivy/TiCIReadTaskPool.h`
- `dbms/src/Storages/Tantivy/TantivyInputStream.h`
- `dbms/src/Storages/StorageTantivy.cpp`

## Current Hardening

- TiFlash validates vector request payloads before crossing the FFI boundary.
- TiFlash decodes float result columns and vector payload columns from TiCI rows.
- TiFlash fails fast if a vector query requests a vector column that TiCI did not materialize.
