# TiFlash Plan: Hybrid Vector Query on TiCI

## Goal

Execute hybrid vector queries through the TiCI read path instead of the legacy ANN path.

Current rollout scope:

- vector-only TiCI queries first
- pushed-down `filter + vector` deferred to a later phase
- runtime validation should currently avoid add-index-on-existing-data.
  `import into` / backfill for hybrid-vector data is not adapted yet; use
  empty-table DDL plus CDC writes for e2e.
- the local validation harness should now reuse the playground-managed TiCDC
  changefeed instead of creating a second one manually.

TiFlash should support:

- fulltext / expression TiCI queries
- vector-only TiCI queries
- vector + pushed-down filter TiCI queries later

## Current State

- The current TiCI path already handles `FULLTEXT INDEX`.
- The same expression path already carries hybrid inverted/scalar predicates.
- TiCI vector search is now wired in the TiFlash local/remote TiCI path.
- The TiCI executor path now accepts either `IndexScan.fts_query_info` or `IndexScan.tici_vector_query_info`.
- The legacy ANN path still exists in DeltaMerge, but it is not the target for hybrid vector.
- Current upstream rollout does not populate `filter_expr` for TiCI vector queries yet.
- Local macOS build validation passed on `2026-03-19` in `cmake-build-codex-release`; the `dbms/src/Server/tiflash` binary was built successfully.
- Runtime smoke/e2e validation passed on `2026-03-20` with
  `playground:v1.16.2-feature.fts`, `upstream/vector@db0a4054`, and the
  playground-managed changefeed.

## Chosen Direction

Treat TiCI scan as two execution modes:

- expression/fulltext mode
  - current `FTSQueryInfo`
  - calls Rust `search(...)`
- vector mode
  - new `TiCIVectorQueryInfo`
  - calls Rust `search_vector(...)`

For vector mode, `filter_expr` stays part of the long-term payload shape, but the current rollout only requires the vector-only subset.

## Prerequisites

- ✅ TiCI `search_vector` FFI ready on `upstream/vector@db0a4054`
- ✅ tipb `TiCIVectorQueryInfo` proto merged (commit `a25a67b`)
- ✅ TiDB planner populates `TiCIVectorQueryInfo` on `tipb.IndexScan` (PR #67103)
- ✅ Update `contrib/tici` submodule to vector-capable upstream

## TiFlash Work Items

1. ✅ Generalize TiCI scan parsing. (PR7)
   - `TiCIScan` parses either `FTSQueryInfo` or `TiCIVectorQueryInfo` via `TiCIQueryMode` enum.
   - `PhysicalPlan::buildTiCIScan()` accepts both modes.

2. ✅ Generalize TiCI read task creation. (PR7)
   - `TiCIReadTaskPool` has separate FTS/Vector constructors.
   - `VectorQueryState` holds col_id, distance_metric, query_vector, and optional filter_expr.
   - Reuses `tipbToTiCIExpr` for vector filter conversion.

3. ✅ Add vector FFI call path. (PR7)
   - `TantivyInputStream` branches to `search(...)` or `search_vector(...)`.
   - `VectorSearchParam` uses `(col_id, distance_metric)` matching TiCI FFI.

4. ✅ Basic hardening for vector-only rollout.
   - Validate `top_k`, `column_id`, `dimension`, metric, and `query_vector` byte length before FFI call.
   - Reject non-finite query vector values early in TiFlash.
   - Decode `FLOAT/DOUBLE` result fields correctly.
   - Decode `Array(Float32)` vector payloads when TiCI materializes them.
   - Fail fast if a TiCI vector query requests a vector column but TiCI does not return the payload.

5. 🔲 Add tests / integration validation. (PR8)
   - local TiCI vector query
   - remote TiCI vector query
   - malformed vector payload rejection
   - returned float/vector column materialization
   - vector + filter query later

## Performance/Stability Requirements

- Avoid reading/materializing full rows before TiCI shard-level pruning.
- Keep top-k reduction shard-local as long as possible.
- Ensure remote-read behavior matches local-read semantics.
- Reject invalid or ambiguous payloads early.
- Add metrics for TiCI vector local/remote reads, filter selectivity, and shard query latency.

## Expected PR Split For TiFlash

Recommended TiFlash split: 2 PRs.

1. Query mode, executor, and FFI wiring.
2. Hardening, tests, and optional later filter pushdown.
