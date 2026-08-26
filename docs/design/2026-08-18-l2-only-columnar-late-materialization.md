# L2-Only Late Materialization for Columnar Reads

- Author(s): TBD
- Status: Implemented (default off)
- Last Updated: 2026-08-26
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Terminology](#terminology)
- [Current Implementation](#current-implementation)
- [Detailed Design](#detailed-design)
  - [Scope and Eligibility](#scope-and-eligibility)
  - [Column Projections](#column-projections)
  - [Hybrid Batch Representation](#hybrid-batch-representation)
  - [Source Reader Behavior](#source-reader-behavior)
  - [L2 Physical Row References](#l2-physical-row-references)
  - [Merge and MVCC](#merge-and-mvcc)
  - [Exact Predicate Evaluation](#exact-predicate-evaluation)
  - [Late Pack Planning](#late-pack-planning)
  - [Variable-Width Column Packs](#variable-width-column-packs)
  - [FFI Protocol](#ffi-protocol)
  - [Pending Batch State Machine](#pending-batch-state-machine)
  - [Cost Model and Metrics](#cost-model-and-metrics)
  - [Implementation Mapping](#implementation-mapping)
- [Correctness Invariants](#correctness-invariants)
- [Compatibility](#compatibility)
- [Failure Handling](#failure-handling)
- [Test Design](#test-design)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Rollout Plan](#rollout-plan)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This implementation adds late materialization to the cloud columnar read path, but only
for rows read by the level-2 (L2) `ColumnarConcatReader`. Memtable rows,
unconverted L0 row data, and L0/L1 columnar files continue to read all requested
columns eagerly. A scan does not need to contain only L2 data.

The optimization divides requested physical columns into an early projection
and a late projection. All sources participate in the existing global merge and
MVCC pass using the early projection. Every row that survives MVCC carries
either an eager late-value reference or an L2 physical-row reference. TiFlash
evaluates the exact predicate over the merged, MVCC-visible early rows. The Rust
reader then combines selected eager values with late values loaded from L2.

The late L2 I/O unit is a complete pack of one late column. A pack is read once
if it contains at least one selected row and is skipped completely otherwise.
The late path never calls `ColumnarColumnReader::set_row_idx()`. Pack-local row
selection happens after the complete pack has been read, decrypted, and
decompressed.

## Motivation or Background

The current columnar path constructs a complete Rust `Block` before it crosses
the FFI boundary. TiFlash then deserializes all requested columns and executes
the exact pushed-down predicate. For a selective query over a wide table, most
bytes in non-predicate columns may therefore be read, decompressed, serialized,
copied across FFI, and deserialized only to be filtered out immediately.

L2 is the best initial scope for this optimization:

- L2 files are consumed by `ColumnarConcatReader` in key order.
- L2 contains most of the data in the target deployments. This is an expected
  workload property and must be validated by level-specific metrics; it is not
  a format invariant.
- L2 rows can be identified by a snapshot-pinned file and an absolute physical
  row index.
- L0/L1 and row sources can retain their current eager I/O behavior, which
  avoids implementing random late access for every source type.

The restricted scope does not remove the need for a hybrid merge protocol.
`ColumnarMergeReader` merges all sources by handle and version, and
`ColumnarMvccReader` decides which version is visible. Exact predicate selection
must happen after both operations.

For example:

```text
L1: handle=42, version=200, predicate=false
L2: handle=42, version=100, predicate=true
read_ts >= 200
```

The L1 row suppresses the older L2 row during MVCC, even though the L1 row does
not satisfy the predicate. Filtering the L2 row before the global merge would
incorrectly return version 100. The required order is:

```text
memtable/L0/L1: full data retained in an eager sidecar
L2 concat:       early columns plus a physical-row reference
                         |
                  global merge and MVCC
                         |
              TiFlash exact predicate selection
                         |
       eager gather plus selected complete-pack L2 reads
                         |
                 final TiFlash block assembly
```

## Goals

1. Avoid reading complete L2 late-column packs that contain no selected rows.
2. Avoid serializing unselected late values across FFI for all source types.
3. Preserve the current global merge order and MVCC visibility semantics across
   memtable, unconverted L0, L0/L1 columnar files, and L2 files.
4. Reuse TiFlash's current expression analyzer and execution semantics for exact
   predicate evaluation.
5. Support fixed-width and variable-width late columns without assuming their
   pack boundaries are aligned.
6. Keep the legacy full-materialization reader and FFI contract available as a
   safe fallback.
7. Make performance benefits and regressions observable by source level,
   column kind, and touched-pack ratio.

## Non-Goals

- Late materialization for memtable, unconverted L0, or L0/L1 columnar data.
- A requirement that a scan, range, or batch contain only L2 rows.
- Row-granular or byte-range reads within a compressed column pack.
- Calling `set_row_idx()` once per selected row or range.
- Reusing handle-pack identities for string, bytes, or JSON columns.
- Implementing a second SQL expression evaluator in `kvengine`.
- Supporting ANN, vector-distance projection, or FTS readers in the first
  version.
- Changing the columnar on-disk format or compaction rules.

## Terminology

- **Early column**: a physical column required by merge/MVCC, exact predicate
  evaluation, or both.
- **Late column**: a requested output column not needed to produce the exact
  predicate selection.
- **Eager row**: a row from memtable, unconverted L0, or an L0/L1 columnar file.
  Its late values are read during the early phase and retained in Rust.
- **Deferred row**: an L2 row whose late values are represented by a physical
  row reference until selection is known.
- **Relevant pack**: a late-column pack containing at least one deferred row in
  the pending batch before exact selection.
- **Touched pack**: a relevant pack containing at least one selected deferred
  row.
- **Pending batch**: the Rust-owned early or materialized state between
  `read_early` and either `finish_materialized_block` or `discard_batch`.

## Current Implementation

The implemented legacy read sequence is:

```text
RNColumnarInputStream
  -> CloudStorageEngineInterfaces::fn_read_block
  -> CloudColumnarReader / CloudColumnarReaders
  -> ColumnarMvccReader
  -> ColumnarMergeReader
       -> row readers for memtable and unconverted L0
       -> one ColumnarTableReader per L0/L1 file
       -> one ColumnarConcatReader over L2 files
  -> complete Rust Block
  -> per-column FFI serialization and TiFlash deserialization
  -> extraCast
  -> exact pushed-down filter
```

When LM is enabled for one reader work, `RNColumnarInputStream` uses the
optional two-phase extension instead. It reads and deserializes only the early
header columns, evaluates a `FilterTransformAction`, passes its exact
selection to CSE, obtains selected late columns, and then rebuilds the original
header. The existing downstream filter remains in the pipeline and is applied
again to the assembled block; LM uses the first evaluation only to avoid late
column work, not to change filter ownership.

The relevant implementation is summarized below.

| Responsibility            | Current code                                                                                                                                                                                               | Relevant behavior                                                                                                     |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| TiFlash block read        | [`StorageDisaggregatedColumnar.cpp`](../../dbms/src/Storages/StorageDisaggregatedColumnar.cpp)                                                                                                             | `RNColumnarInputStream::readImpl` calls `fn_read_block`, then fetches and deserializes every header column.           |
| Exact filter construction | [`StorageDisaggregatedColumnar.cpp`](../../dbms/src/Storages/StorageDisaggregatedColumnar.cpp), [`InterpreterUtils.cpp`](../../dbms/src/Flash/Coprocessor/InterpreterUtils.cpp)                            | External filter conditions and `TableScan` pushed filters are combined and executed with `DAGExpressionAnalyzer`.     |
| Reader construction       | [`read.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/read.rs)                                                                                                                            | `new_columnar_mvcc_reader_impl` builds row, L0/L1 table, L2 concat, merge, and MVCC readers.                          |
| Multi-table buffering     | [`read.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/read.rs)                                                                                                                            | `CloudColumnarReaders` and `BlockResult` eagerly extract every column from a completed block in concurrent mode.      |
| Global merge              | [`reader.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs)                                                                                                         | `ColumnarMergeReader::read` appends rows in handle-ascending and version-descending order.                            |
| MVCC                      | [`reader.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs)                                                                                                         | `ColumnarMvccReader::try_read_block` removes future versions, older versions, tombstones, and rows beyond the range.  |
| L2 concatenation          | [`reader.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs)                                                                                                         | `ColumnarConcatReader` finishes one L2 `ColumnarTableReader` before advancing to the next file.                       |
| L2 ordering               | [`columnar.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/columnar.rs)                                                                                                     | `ColumnarLevel::sort` sorts level 2 by each file's smallest key.                                                      |
| Pack positioning          | [`reader.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs), [`columnar.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/columnar.rs) | `set_row_idx` uses `PackOffsets::search_pack_idx`, which is a forward linear search followed by pack loading.         |
| Pack construction         | [`builder.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/builder.rs)                                                                                                       | Fixed-width columns follow handle pack completion; variable-width columns split independently by row and byte limits. |
| Rough filtering           | [`filter.rs`](../../contrib/cloud-storage-engine/components/kvengine/src/table/columnar/filter.rs)                                                                                                         | Rust evaluates pack-level min/max conditions, not exact row-level SQL predicates.                                     |

Changing only `CloudColumnarReader::ffi_read_column` cannot provide the desired
optimization. At that point the complete Rust block has already loaded and
decompressed all requested L2 columns.

## Detailed Design

### Scope and Eligibility

The implementation enables LM per `RNColumnarInputStream`, rather than once
for an entire query. It requires all of the following:

1. `enable_columnar_l2_late_materialization` is true.
2. The weak symbol `tiflash_columnar_get_late_materialization_interfaces` is
   present and returns ABI version 1, a descriptor at least as large as the
   v1 descriptor, and every required callback.
3. At least one TableScan pushed-down filter exists. The exact condition set is
   `filter_conditions.conditions` followed by the TableScan pushed filters.
4. The exact condition set has a non-empty physical column dependency set, and
   the source header has at least one late column.
5. The exact predicate does not reference the synthetic extra table-ID column
   or a generated column. `TIME`, and `TIMESTAMP` outside UTC, are also
   rejected because the local early-action representation has not been proven
   equivalent for those cases.
6. `late_column_count / max(1, early_column_count - 2)` is strictly greater
   than `columnar_l2_late_materialization_min_late_to_early_ratio`. The two
   subtracted early columns are handle and version.
7. CSE accepts the reader: it rejects ANN/FTS, multi-table sequential reads,
   worker aggregation, missing schema/L2 files, missing handle/version in the
   early projection, and overlapping L2 files for the target table.

After CSE has produced the first non-empty early batch, TiFlash evaluates the
exact selection. If its skip ratio is below
`columnar_l2_late_materialization_min_selection_skip_ratio`, TiFlash discards
that pending LM batch, disables LM for this input stream, and immediately reads
the next block through the legacy ABI. This is a runtime probe, not a
creation-time byte or pack cost model.

Memtable, unconverted L0, and L0/L1 data do not disable LM. They produce eager
rows in the same hybrid batch. A capable reader can also produce an eager-only
batch; it remains valid and still uses the two-phase protocol. Pack-clean is
not selected by an LM-specific chooser: the hybrid reader deliberately does not
use pack-clean because it requires real handle and version values.

### Column Projections

Reader creation derives three ordered sets from the existing physical source
header:

```text
full_output_columns = physical columns currently returned by the source
predicate_columns   = transitive physical dependencies of the exact predicate
early_input_columns = internal handle/version plus predicate_columns
early_output_columns = full_output_columns intersect early_input_columns
late_columns         = full_output_columns minus early_output_columns
```

The internal handle and version are always early because merge and MVCC require
them, even when they are not part of the query output. All sources use the same
early schema so that `ColumnarMergeReader` continues to compare homogeneous
blocks. The original output order is stored separately and used during final
assembly.

Column identities are physical column IDs, not names or positions. PK-handle
aliases and internal handle/version columns must be normalized during plan
construction. Default-value columns remain valid late columns; they generate
selected values by count and require no pack read.

### Hybrid Batch Representation

The merge path gains an LM-only representation. The existing `Block` and
normal `read` methods remain unchanged.

```rust
enum MaterializationRef {
    Eager {
        eager_row_idx: u32,
    },
    L2Deferred {
        l2_reader_id: u32,
        file_index: u32,
        physical_row_idx: u32,
    },
}

struct HybridBatch {
    early: Block,
    refs: Vec<MaterializationRef>,
    eager_late: Vec<ColumnBuffer>,
}
```

`early.rows() == refs.len()` is mandatory. `eager_late` contains only late
columns and only rows copied from eager sources. `eager_row_idx` indexes the
batch-owned `ColumnBuffer` vectors.

The batch-owned eager block is deliberate. A raw `{source_id,
source_block_row}` reference is unsafe because `ColumnarMergeReader` may exhaust
and refill a source more than once while constructing one output batch. At the
same append point where an eager early slice is added to the merged block, its
late slice is appended to `eager_late` and stable indexes are emitted. Source
buffers may then refill normally.

After MVCC, the implementation retains the eager sidecar and its stable
`eager_row_idx` values; it filters only `early` and `refs`. Exact selection
builds `HybridMaterializationPlan` once, mapping selected output rows to either
an eager sidecar index or an L2 deferred slot. No eager-sidecar compaction or
index rewriting occurs.

### Source Reader Behavior

| Source                                 | Early phase                                                                                                    | Materialization reference | Late phase                                                                    |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| Memtable and unconverted L0 row reader | Decode the same full row as today; append early columns and copy late values into the batch sidecar.           | `Eager`                   | Gather selected rows from `eager_late`; no second source read.                |
| L0/L1 `ColumnarTableReader`            | Read the same full requested block as today; append early columns and copy late values into the batch sidecar. | `Eager`                   | Gather selected rows from `eager_late`; no random access.                     |
| L2 `ColumnarConcatReader`              | Read handle, version, and early columns only.                                                                  | `L2Deferred`              | Read complete touched packs for every late column and gather selected values. |

Only L2 avoids late-column I/O. Eager sources may still benefit from not
serializing unselected late rows across FFI, but their disk/decode work is not
reported as an LM I/O saving.

### L2 Physical Row References

`ColumnarTableReader::read_with_physical_rows` is the LM companion API. It
creates an early block and returns physical rows while preserving normal
`read` behavior. `ColumnarConcatReader::read_with_physical_rows` attaches its
stable file index to those rows.

For each appended L2 row, the early reader records the absolute row index in
that file/table. The index is derived from the aligned version reader (or handle
reader) pack's absolute row start plus its pack-local cursor before the append;
it is not derived from a variable-width predicate column. Packs rejected by
rough filtering produce no rows and no references. `Unknown` rough-filter
results are still read.

`ColumnarConcatReader` assigns a stable `l2_reader_id` and a stable index in its
snapshot-pinned, table-filtered file vector. A batch may contain references to
multiple L2 files because merge output can cross a concat boundary or interleave
L1 and L2 rows. Late reading groups references by `(l2_reader_id, file_index)`;
it does not relocate rows by handle.

Late accessors are built from the pinned `ColumnarFile` and column metadata, not
from the concat reader's current cursor. Advancing the concat reader therefore
does not invalidate an earlier physical reference.

### Merge and MVCC

`HybridMergeReader` is a dedicated counterpart to `ColumnarMergeReader`. It
uses the same handle-ascending/version-descending heap ordering, but emits
`HybridBatch`: eager sources retain their late `ColumnBuffer` sidecars and L2
sources emit `L2Deferred` references. Its comparator and heap behavior do not
change.

`ColumnarMvccState::apply_hybrid_visibility` reuses the normal reader's
visibility-range calculation. The normal path applies that result to a complete
`Block`; the hybrid path applies it to:

- the early block;
- the materialization-reference vector; and

The eager sidecar is intentionally not compacted.

There must not be separate implementations of read-ts checks, same-handle
deduplication, tombstone handling, range-end handling, or int/common-handle
logic. Predicate selection is produced only after this shared MVCC step.

### Exact Predicate Evaluation

Rust rough filtering remains a pack-elimination optimization. It must not be
extended into an exact SQL evaluator. Exact selection remains owned by
TiFlash's `DAGExpressionAnalyzer` and the current expression actions.

When LM is enabled, `RNColumnarInputStream` builds one cached
`FilterTransformAction` for that input stream. It copies and remaps the same
combined condition set used by `filterConditionsWithPushedDownFilters`:

```text
filter_conditions.conditions AND table_scan.getPushedDownFilters()
```

`ColumnRef` operands in the copied expressions are remapped from the TableScan
column index to the early-header index. The original protobuf expressions are
not mutated. For each pending batch, TiFlash performs the following steps:

1. Deserialize the early physical columns into `early_block`.
2. Copy it into `evaluation_block` and execute the cached filter action there.
3. Send `All`, `None`, or the resulting `UInt8` filter to CSE with the batch ID.
4. Apply that same selection to the unmodified early columns.
5. Deserialize selected late columns and assemble the original full source
   header.

The evaluation copy prevents predicate-only casts and temporary columns from
changing the early columns used in final assembly. `action.fill` still adds the
physical table ID, and the existing downstream filter is intentionally retained
in both stream and pipeline paths. Therefore an LM-result block is filtered
once for selection and again by the normal pipeline. The second application is
the current compatibility guard; it must not be removed without an explicit
semantic-equivalence and profiling change.

If a reader is ineligible before its first LM batch, `readImpl` uses the legacy
full-block ABI. If the first exact-selection probe is too dense, TiFlash
discards that batch and makes the same per-stream fallback. Neither fallback
changes downstream filter ownership.

### Late Pack Planning

No selected row invokes `set_row_idx()`. Rust builds a batch-level plan after it
receives the exact selection:

```rust
struct LateReadSlot {
    physical_row_idx: u32,
    selected_output_idx: u32,
}

struct LatePackPlan {
    pack_idx: u32,
    slots: Vec<LateReadSlot>,
}

struct LateColumnPlan {
    column_id: i64,
    packs: Vec<LatePackPlan>,
}

struct LateFilePlan {
    l2_reader_id: u32,
    file_index: u32,
    columns: Vec<LateColumnPlan>,
}
```

Plan construction and execution are:

1. Scan selected materialization references. Eager references are gathered from
   `eager_late`; L2 references are grouped by reader and file. Every selected
   row is assigned its stable `selected_output_idx` in merged/MVCC order.
2. For each late column independently, map every physical row to a pack using
   that column's `ColumnMeta.pack_offsets.find_pack_idx`, which binary-searches
   that column's `row_offsets`. It does not use the cursor-oriented
   `search_pack_idx`.
3. Coalesce slots with the same `(file, column, pack)` into one
   `LatePackPlan`. Sort pack plans by pack index to preserve sequential access
   opportunities.
4. Construct the late `ColumnarColumnReader` or dedicated
   `LateColumnAccessor` with `packs_filter = None`.
5. Load, decrypt, and decompress each touched pack exactly once for the pending
   batch. Extract selected pack-local rows from the complete `ColumnBuffer`.
6. Store fragments with `selected_output_idx`, then assemble each late column
   in selected merged/MVCC order together with eager fragments.
7. Serialize only selected rows to TiFlash.

If the selection is empty, Rust releases the pending batch without reading any
late pack. If it selects every row, the same plan remains correct and will touch
all relevant packs. The implementation may coalesce adjacent pack reads in the
future, but the logical accounting and at-most-once rule remain per column pack.

### Variable-Width Column Packs

String, bytes, JSON, and other variable-width columns do not share pack
boundaries with handle or fixed-width columns.

This follows directly from the current builder and reader:

- `ColumnarColumnBuilder::append` passes the handle `finish_pack` decision to
  fixed-width columns.
- `append_var` splits a variable-width column according to its own accumulated
  row count and byte size.
- `ColumnarTableReader::new` sets `packs_filter = None` for columns whose fixed
  size is zero.

Therefore, `physical_row_idx` is the only shared identity across late columns.
Pack identity is scoped to `(file, column)`. The implementation must obey these
rules:

1. Every late column performs its own physical-row-to-pack lookup.
2. A handle or fixed-column pack index is never reused for a variable-width
   column.
3. A fixed-column rough-filter bitmap is never passed to a variable-width late
   accessor.
4. Touched-pack count, compressed bytes, and read ranges are calculated per
   column before being aggregated for the batch.
5. A selected row may map to unrelated pack IDs in two late columns, and the
   plan must represent that normally.

The complete-pack rule is especially important here. If one selected row falls
in a large string pack, the complete string pack is read and decompressed once;
the implementation does not attempt a row-sized I/O operation inside it.

### FFI Protocol

The current `fn_get_columnar_reader`, `fn_read_block`, and per-column read
functions remain unchanged. LM is an implemented optional extension, exported
from the Hub through the weak C symbol
`tiflash_columnar_get_late_materialization_interfaces()`. The v1 descriptor is
validated by its `version`, minimum `size`, and all callback pointers; it does
not change `CloudStorageEngineInterfaces` or `RaftStoreProxyFFIHelper` layout.
Its callback contract is:

```text
read_early_block(reader, limit, early_column_ids, &batch_id, &physical_table_id)
  -> row_count | 0 (EOF) | UINT64_MAX (error)

read_early_column(reader, batch_id, column_id)

materialize_selected(reader, batch_id, selection_kind, selection_bytes)
  -> selected_row_count | UINT64_MAX (error)

read_late_column(reader, batch_id, column_id)
finish_materialized_block(reader, batch_id) -> 1 | 0
discard_late_materialization_batch(reader, batch_id) -> 1 | 0
is_late_materialization_supported(reader, early_column_ids) -> 1 | 0
```

`selection_kind` is `All`, `None`, or `Bytes`. `Bytes` uses the existing
TiFlash filter representation: one `UInt8` per input row, where zero is false
and non-zero is true. `All` and `None` carry no payload and avoid allocating a
uniform selection buffer.

The protocol requirements are:

- LM entry points are used only when every required function pointer is
  non-null and the interface version is supported.
- `batch_id` is unique within a reader instance and is validated by every batch
  operation.
- For `Bytes`, CSE validates that payload length equals the pending
  MVCC-output row count. `All` and `None` have an empty payload.
- `materialize_selected` returns the number of selected rows: the number of
  non-zero bytes for `Bytes`, the pending row count for `All`, and zero for
  `None`.
- `read_late_column` is valid only after successful materialization and before
  the batch is finished. Each late column can be taken only once.
- Every returned early and late column is validated against its expected row
  count after TiFlash deserialization.
- Early and late Rust buffers use the existing Rust GC ownership convention.
  TiFlash releases each returned buffer after deserialization.
- `physical_table_id` is attached to the early batch and cannot change while it
  is pending.
- A reader that has returned a non-empty early batch cannot switch to the
  legacy reader for that batch. The only density fallback discards the first
  probe batch before reading a separate legacy block.

The FFI header, generated Rust bindings, proxy implementation, and columnar Hub
are updated together. Future descriptor revisions must retain this lockstep
rule. A binary without the complete LM extension continues to use the legacy
full-materialization path.

### Pending Batch State Machine

Each Rust reader permits at most one globally merged pending batch:

```text
Idle --read_early(non-empty)------> PendingEarly(batch_id)
Idle --read_early(EOF)------------> Drained
PendingEarly --materialize(valid)-> Materialized(batch_id)
PendingEarly --discard(valid)-----> Idle
Materialized --finish(valid)------> Idle
Materialized --discard(valid)-----> Idle
PendingEarly/Materialized --drop/cancel/error--> caller discards, then Idle
```

While either batch state is active, the reader rejects another early read,
seek, reset, or range switch. `PendingEarly` owns the early buffers, references,
eager sidecar, and pinned L2 metadata. Successful materialization replaces that
state with `Materialized`, which owns the selected late buffers until TiFlash
takes every expected late column and calls `finish_materialized_block`. Because
eager values are copied into the sidecar during merge, source readers may refill
while the batch is being constructed; they simply cannot be advanced by a
second batch after the first batch becomes pending.

TiFlash keeps a scope guard armed from early-batch acquisition through
`finish_materialized_block`; any exception before finish calls
`discard_late_materialization_batch`. CSE returns protocol or storage errors to
the caller; it has no persistent `Failed` state. A partially assembled block is
not returned because TiFlash throws before final assembly and the scope guard
discards the pending batch.

### Cost Model and Metrics

Row selectivity alone is not a reliable estimate because late I/O is saved only
when complete packs are skipped. For every late column, the implementation
uses the following batch-local quantities:

```text
relevant_packs = packs containing any deferred row in the pending batch
touched_packs  = relevant packs containing any selected deferred row

touched_pack_ratio = touched_packs / relevant_packs
touched_byte_ratio = compressed bytes of touched packs
                     / compressed bytes of relevant packs
```

The implemented admission controls are intentionally simpler than a byte-cost
model: the early/late column-count ratio is checked before reading, and the
first exact batch must satisfy the configured skip ratio. A dense probe is
discarded and the reader switches to legacy mode for its remaining lifetime.

The currently exported metric is
`late_materialization_skipped_packs`. For each late column, CSE counts candidate
packs referenced by deferred rows minus packs actually loaded for selected
rows, and aggregates the result in `ColumnarRuntimeStats` and TiFlash's
`ColumnarScanContext`. Existing read, serialization, rough-filter and
deserialize timings remain available. Per-column byte ratios, eager-sidecar
bytes, repeated-load counters, and pending-batch memory metrics are not
implemented yet and must not be treated as rollout signals.

### Implementation Mapping

| Area                                  | Implemented behavior                                                                                                                                                                                                                                                        |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `components/kvengine/src/read.rs`     | Builds `HybridMvccReader`, owns the pending-batch protocol, validates IDs and selections, and materializes all late columns concurrently (bounded to four) on the first late-column request.                                                                                |
| `table/columnar/reader.rs`            | Defines `HybridBatch`, `HybridMergeReader`, `HybridMvccReader`, physical L2 references, `HybridMaterializationPlan`, and `LateColumnAccessor`. Normal readers remain available.                                                                                           |
| `table/columnar/columnar.rs`          | Provides `PackOffsets::find_pack_idx`, the binary physical-row-to-pack lookup.                                                                                                                                                                                              |
| `StorageDisaggregatedColumnar.cpp/.h` | Derives early IDs, checks TiFlash-side eligibility, resolves the optional ABI, executes the cached filter action, probes selection density, calls the two-phase protocol, and preserves the legacy reader plus downstream filter.                                              |
| Settings                              | Adds the default-off `enable_columnar_l2_late_materialization`, the minimum early/late ratio (default `10.0`), and the minimum first-batch skip ratio (default `0.5`).                                                                                                      |
| Hub FFI                               | Exports the v1 extension descriptor and bridges callbacks to `CloudColumnarReaders`; the main Cloud Storage Engine interface remains ABI-stable.                                                                                                                              |
| Build dependencies                    | `contrib/tiflash-proxy-cmake/CMakeLists.txt` tracks local CSE Rust/TOML/proto inputs when next-gen columnar is enabled, so a CSE change rebuilds the linked Hub.                                                                                                            |

## Correctness Invariants

The implementation must assert or test the following invariants:

1. Exact selection is evaluated in the row space produced by the global merge
   and MVCC pass, never in a source-local row space.
2. Each MVCC-visible row has exactly one `MaterializationRef`.
3. `HybridBatch.early.rows() == HybridBatch.refs.len()` before and after MVCC.
4. MVCC applies the same retained ranges to early rows and references.
5. Each selected row produces exactly one late value for every late output
   column, whether the row is eager or deferred.
6. Final row order is merged/MVCC order restricted by the exact TiFlash
   selection. File/column/pack read order never changes output order.
7. An L2 physical row reference is resolved only against its snapshot-pinned
   file and table metadata.
8. Pack identity is per `(file, column)`; only physical row identity is shared
   across columns.
9. A touched `(file, column, pack)` is loaded at most once per pending batch,
   and an untouched pack is not loaded by the late path.
10. Late access never calls `set_row_idx()`; the early L2 reader can still use
    its existing skip-pack repositioning.
11. Rough filtering may reduce I/O but cannot replace or weaken exact
    filtering. LM selection and the existing downstream filter must use the
    same combined conditions.
12. No pending batch survives reader release, cancellation, retry, or error.

## Compatibility

### Query Semantics

The result rows, order, column types, errors, timezone behavior, collation
behavior, null semantics, and JSON guard behavior must match the current full
reader. The design reuses the current TiFlash expression actions specifically
to avoid semantic drift.

Partition reads remain supported when the predicate does not depend on the
synthetic extra table-ID column. The early batch carries `physical_table_id`,
and final assembly retains the current extra table-ID fill behavior.

Generated output columns may continue to use the current placeholder and later
generation flow. Predicates depending on generated columns are excluded from
V1 because their early dependency and evaluation ordering are not currently
represented by the Rust reader contract.

### FFI and Upgrade Compatibility

Legacy function pointers and behavior remain available. LM is enabled only when
TiFlash observes the complete supported extension. Missing functions, an
unsupported interface version, or a CSE capability rejection selects the full
reader before any early batch is acquired. A dense first batch is separately
discarded before legacy fallback.

All in-process FFI definitions must be regenerated and shipped consistently for
TiFlash, proxy variants, and the columnar hub. Mixed binaries that cannot prove
the extension layout use the legacy interface; they must not infer capability
from one non-null function pointer.

No persistent data format changes are introduced, so downgrade only requires
disabling the feature or running a binary without the extension.

### Other Features

- ANN, vector-distance projection, and FTS use the current reader path.
- Pack-clean and LM are mutually exclusive reader modes.
- Rough filtering remains conservative and may run before exact selection.
- Encryption and IA/remote segment reads remain in the existing `PackLoader`;
  the late accessor changes which complete packs are requested, not how a pack
  is decoded.
- Lock resolution, region retry, snapshot lifetime, and range replanning retain
  their current outer control flow. Pending state must be discarded before a
  reader is released for retry.

## Failure Handling

| Failure                                                             | Required behavior                                                                                            |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Static query is not eligible                                        | Build the existing pipeline and reader.                                                                      |
| FFI extension missing or unsupported                                | Use the legacy full reader; do not acquire an early batch.                                                   |
| Snapshot has no eligible L2 file or the overlap check fails         | Report LM unsupported to TiFlash; it keeps the legacy reader for that reader work.                           |
| Invalid batch ID, selection length, or column request               | Return a protocol error; TiFlash's scope guard discards the pending batch.                                    |
| TiFlash predicate evaluation throws                                 | Discard the pending batch with a scope guard, then rethrow the original exception.                           |
| Empty selection                                                     | Discard the pending batch without late-pack reads and continue to the next early batch.                       |
| Late pack I/O, key lookup, decrypt, checksum, or decompress failure | Propagate the storage error; the scope guard discards pending state.                                           |
| TiFlash late-column row count mismatch                              | Treat as a logical/protocol error and release the reader.                                                    |
| Cancellation or reader drop while pending                           | Abort and release eager sidecar, refs, accessors, and FFI buffers.                                           |
| Dense or fragmented selection                                       | Complete the same pack plan, potentially reading all relevant packs; never switch readers mid-batch.         |

## Test Design

### Functional Tests

Implemented Rust unit coverage includes hybrid early/ref retention, stable eager
sidecar indexes, selected L2 late-value materialization, and the strict L2
non-overlap gate. The following broader cases remain required:

1. L1 version 200 with predicate false and L2 version 100 with predicate true;
   the old L2 version must not appear. Include the reverse predicate outcome,
   future versions, tombstones, and read-ts boundaries.
2. Interleaved memtable, unconverted L0, L0, L1, and multiple L2 files. Compare
   early rows, refs, selected late values, and final order with the complete
   reader.
3. Eager sidecar stability when one eager source refills multiple times while a
   merged batch is being built.
4. Sparse, contiguous, empty, and all-row L2 selections across one and multiple
   files and packs.
5. Fixed-width, nullable, default, decimal, string, bytes, and JSON late
   columns.
6. Fixed and string pack misalignment produced with a small byte limit. Verify
   that string rows use the string column's own offsets and no fixed-column
   rough-filter bitmap.
7. One complete load per touched `(file, column, pack)`, zero loads for
   untouched packs, and no `set_row_idx` call from late access.
8. L2 rough-filter `None` and `Unknown` results mixed with eager rows, with no
   false negative.
9. Invalid state transitions, duplicate materialization, bad selection sizes,
   wrong batch IDs, late I/O failure, cancellation, and drop.
10. Gating for no predicate, no late column, no L2, overlapping L2 ranges, ANN,
    FTS, vector projection, generated/extra-table-ID predicate, pack-clean,
    missing FFI, and internal concurrent mode.

TiFlash unit and integration tests still need to cover:

1. Early projection extraction and original-header reconstruction.
2. Exact selection for AND, OR, NOT, NULL, casts, timestamp/timezone,
   collation, and guarded JSON expressions.
3. Combined external filter conditions and `TableScan` pushed filters in the
   current order, including LM early-block selection and the retained downstream
   filter.
4. Empty/all/partial selections and early/late row-count validation.
5. Full-read local filtering when an individual reader does not support LM,
   while the pipeline uses materialization-action filter ownership.
6. Exception scope guards that discard a pending Rust batch.

### Scenario Tests

End-to-end tests compare feature-on and feature-off results for:

- mixed L1/L2 versions and tombstones;
- partition tables and multiple physical tables;
- multiple key ranges and batches crossing L2 file boundaries;
- region retry, lock error, source cancellation, and reader release;
- local, remote, IA-cached, encrypted, and cache-miss pack reads;
- scans where a batch contains only eager rows even though the reader is LM
  capable.

### Compatibility Tests

- New TiFlash with a complete LM-capable proxy/hub.
- New TiFlash with the LM extension absent or disabled.
- Feature flag toggled off with new binaries.
- Restart and downgrade after LM queries, confirming that no persistent state
  or format migration exists.
- Both legacy stream and pipeline execution models.

### Benchmark Tests

Benchmarks must vary:

- L2 share and eager-source share;
- projected late-column width;
- fixed-width versus variable-width late data;
- row selectivity and actual touched-pack ratio;
- clustered versus scattered selected rows;
- one versus many L2 files;
- local, remote, IA cache-hit, and IA cache-miss reads;
- pack sizes, including packs above the direct-read cache threshold;
- batch size and L1/L2 interleaving frequency.

Report wall time, CPU, peak memory, I/O bytes, decompressed bytes, FFI bytes,
pack metrics, and stage timing. Promotion beyond default-off requires a clear
reduction in L2 late bytes at low touched-pack ratios without material
regression for dense or eager-heavy batches.

## Impacts & Risks

### Expected Impacts

- Selective wide scans can skip complete L2 late-column packs.
- Only selected late values cross FFI, including values from eager sources.
- L0/L1/memtable I/O behavior remains unchanged.
- LM evaluates an additional early-block selection before the normal downstream
  filter, which adds CPU work but preserves existing final filter ownership.
- Rust retains a batch-owned eager late sidecar and L2 metadata until selection
  completes.

### Risks

1. **Dense selection regression:** if almost every relevant pack is touched,
   two-phase calls, early evaluation, sidecar management, and final assembly add
   overhead without L2 I/O savings.
2. **Variable-width amplification:** one selected large string may cause a large
   complete pack read even at low row selectivity.
3. **Memory growth:** eager source payloads and pending early data coexist until
   materialization; mixed-source batches can increase peak memory.
4. **Expression drift:** remapped ColumnRefs or independently rebuilt actions
   could change the selection. The normal downstream filter is retained as a
   compatibility guard; equivalence tests are still required.
5. **State-machine bugs:** cancellation, retry, or exception paths could leave a
   pending batch or stale FFI buffer alive.
6. **Pack-plan bugs:** treating pack IDs as cross-column identities would return
   wrong variable-width values.
7. **Concurrency loss:** disabling `CloudColumnarReaders` internal worker mode
   may offset LM gains for multi-table reads.
8. **Pack-clean regression:** reading real handle/version columns for LM can be
   slower than the existing clean-pack path.
9. **Remote access fragmentation:** many touched packs can create more small
   remote/cache operations even though each pack is loaded only once.

The default-off flag, strict gates, per-reason metrics, and workload-based
rollout are required mitigations.

## Investigation & Alternatives

### Defer All Source Types

This would maximize possible I/O savings but requires stable late access for
memtable, row data, L0, and overlapping L1 files. It also expands physical-row
identity and lifetime rules substantially. L2-only deferral captures the
expected dominant data level with a smaller correctness surface.

### Require Pure-L2 Scans

This avoids an eager sidecar but rejects normal scans containing recent L1 or
memtable updates. More importantly, it is unnecessary: a hybrid batch can keep
eager payloads while preserving one global merge and MVCC pass.

### Filter Each L2 File Before Merge

This is incorrect because newer eager or L1 versions must suppress older L2
versions before predicate filtering. The version-200/version-100 example in the
background section demonstrates the failure.

### Reevaluate the Exact Predicate in Rust

The current Rust filter parser is a conservative pack-level rough filter. A
second row-level evaluator would need to reproduce TiFlash casts, collations,
timezone behavior, null semantics, JSON guards, and errors. Reusing TiFlash's
current expression actions is both smaller and safer.

### Reposition with `set_row_idx()`

Calling `set_row_idx()` for selected rows or short ranges can repeatedly search
pack metadata and load/decode packs. It is also difficult to reason about when
fixed and variable-width packs are misaligned. The proposed plan maps rows once
per column, loads each touched pack once, and extracts locally.

### Use Handle Pack IDs for Every Late Column

This is invalid for variable-width columns because their pack boundaries are
independent. Physical row index is the cross-column identity; pack index is not.

### Read Full L2 Late Columns and Filter Only at FFI

This reduces serialization but does not save storage reads, decryption,
decompression, or Rust block memory. It may be a useful fallback behavior for
eager sources, but it does not meet the primary goal.

### Row-Sized Reads Inside a Pack

Compressed packs are the existing independent decode unit. Reading a row-sized
fragment cannot decode the row without the rest of the pack and would add
format-specific random I/O complexity. Complete touched packs are the V1 unit.

## Rollout Plan

1. **Completed: protocol and L2 implementation.** Capability plumbing, hybrid
   batches, the one-pending-batch protocol, L2 complete-pack access, dense-probe
   fallback, and the default-off settings are implemented.
2. **Current: controlled validation.** Validate touched-pack savings,
   dense-selection regressions, variable-width behavior, remote/IA access, and
   the simple ratio/skip-ratio gates against end-to-end workloads.
3. **Future: limited production rollout.** Enable by workload or tenant
   allowlist, monitor rejection reasons, errors, memory, and stage latency, and
   retain an immediate runtime disable switch.
4. **Future: broader enablement.** Consider default-on only after correctness
   equivalence and regression thresholds are met. Extending deferral to other
   levels requires a separate proposal.

## Unresolved Questions

1. Should the simple early/late ratio and first-batch skip-ratio gates be
   replaced with a pack or byte-aware model?
2. Should a future implementation compact eager sidecar rows after MVCC, or is
   retaining them until batch completion faster and sufficiently bounded?
3. What pending batch byte limit should supplement the existing row batch size
   for wide eager-source data?
4. Can internal `CloudColumnarReaders` concurrency later preserve one pending
   batch per worker without eagerly draining columns into `BlockResult`?
5. Should adjacent touched packs be coalesced into larger remote reads while
   retaining per-pack decode and metrics?
6. Which generated-column and hidden-column predicates can be admitted after
   proving an early evaluation header equivalent to the current pipeline?
7. How should source-level exact-filter profile time be attributed so query
   profiles remain comparable with the current downstream filter operator?
