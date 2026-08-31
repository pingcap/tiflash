# Trim Min-Max Index for DATE / DATETIME / TIMESTAMP in Next-Gen Columnar (CSE)

- Status: Draft
- Date: 2026-08-31
- Related: [2026-07-14-trim-minmax-for-date-types.md](./2026-07-14-trim-minmax-for-date-types.md) (DeltaMerge / DMFile)
- Primary change surface: `contrib/cloud-storage-engine` (`kvengine` columnar)

## Summary

This document ports the DeltaMerge `trim_minmax` optimization to TiFlash **next-gen columnar** mode (`ENABLE_NEXT_GEN_COLUMNAR=ON`), where pack-level min-max and rough-check live in Cloud Storage Engine (CSE) rather than in DMFile.

The problem is identical: sparse sentinel timestamps such as `2100-01-01 00:00:00` inflate ordinary pack min-max and destroy pruning for narrow recent-time predicates. The correctness foundation (effective interval `E`, trimmed low/high marks, per-predicate eligibility, conservative `None` correction) is intentionally the same as the DeltaMerge design.

The first CSE version adopts these decisions:

1. Reuse the same default half-open effective range `E = [1900-01-01 00:00:00, 2099-12-01 00:00:00)` and the same packed `u64` temporal encoding already used by CSE columnar values.
2. Keep the ordinary L2 `MinMaxIndex` unchanged. Build an optional **trim** min-max for supported temporal user columns during the same pack traversal.
3. Persist trim as an **optional trailer** appended after the ordinary min-max payload inside the existing `ColumnMeta.compressed_min_max_pack`. Old readers parse only the ordinary prefix and ignore trailing bytes; they do not need a new `ColumnMeta` field layout.
4. Encode the trim payload's per-pack flags in a single `pack_marks` byte: bit 0 = null, bit 1 = trimmed low, bit 2 = trimmed high, bit 3 = has_value (in-range). Unlike DeltaMerge's trim index, CSE does **not** store a separate `has_value_marks` array in the trim trailer.
5. Perform trim eligibility and rough-check selection entirely inside CSE `FilterOperator` (Rust). TiFlash RN continues to push tipb filters and apply TIMESTAMP literal UTC normalization; it does not load CSE min-max bytes.
6. CSE rough-check results remain `None` / `Some` / `Unknown` and only skip pack I/O. There is **no** `All` that skips row-level filtering on the RN. Therefore CSE only needs `None → Some` correction when a trimmed value must match; the DeltaMerge `All → Some` path is not required for correctness in v1.
7. Write trim only on **columnar L2** (same gate as ordinary min-max). L0/L1, non-temporal columns, disabled config, and ineligible predicates always fall back to ordinary min-max (or `Some` when no index exists).
8. Default-disable trim **reads** via `TableScanCtx::enable_trim_minmax`; L2 writes always build trim indexes for supported temporal columns when outliers exist.

## Background

### Current CSE Columnar Min-Max

Relevant code:

- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/columnar.rs` — `MinMaxIndex`, `ColumnMeta`, `TableMeta`
- `.../columnar/builder.rs` — `ColumnarColumnBuilder::finish_pack` / `compute_min_max` / `finish_min_max_pack`
- `.../columnar/filter.rs` — tipb → `FilterOperator` → `rough_check_pack`
- `.../columnar/reader.rs` — skip packs with `FilterOpResult::None`

Write path (L2 only, `level == MAX_COLUMNAR_LEVEL`):

```text
ColumnarTableBuilder.append
  -> ColumnarColumnBuilder::finish_pack(del_marks)
       -> has_null = any (null && !deleted)
       -> compute_min_max: skip deleted || null; min/max over remaining
       -> MinMaxIndex push min/max + marks
  -> finish_min_max_pack
       -> MinMaxIndex::write_to -> compress_pack
       -> ColumnMeta.compressed_min_max_pack
```

On-disk ordinary payload (then LZ4-compressed into `compressed_min_max_pack`):

```text
ColumnBuffer(length = 2 * num_packs)   // min at 2i, max at 2i+1
has_null_marks[num_packs]              // currently 0/1 only
has_value_marks[num_packs]             // currently 0/1 only
```

`ColumnMeta` layout:

```text
PackOffsets
col_info_len | tipb::ColumnInfo
min_max_idx_len | compressed_min_max_pack   // 0 => no index
column_props_len | column_props             // currently always length 0;
                                            // parse hard-skips 4 bytes
```

Query path:

```text
TiFlash StorageDisaggregatedColumnar
  -> normalize TIMESTAMP compare literals to UTC
  -> serialize tipb filters to CSE
CSE FilterOperator::rough_check(TableMeta)
  -> per pack: MinMaxIndex.check_* -> FilterOpResult::{None,Some,Unknown}
ColumnReader::load_pack
  -> None: skip I/O
  -> Some/Unknown: load pack
TiFlash RN
  -> always applies row-level filters (CSE does not claim All)
```

### Problem Scenario

Same as the DeltaMerge design: with pack size 8,192 and one `2100-01-01` sentinel per 10,000 rows, about 56% of packs have `max = 2100`, so narrow recent-range predicates lose most ordinary min-max pruning power.

### Why a Separate Columnar Design

| Dimension | DeltaMerge | CSE columnar |
| --- | --- | --- |
| Owner of index I/O | TiFlash C++ `DMFilePackFilter` | CSE Rust `FilterOperator` / `ColumnReader` |
| Storage | Separate `.idx` / `.trim.idx` + `ColumnStat` protobuf | Embedded `compressed_min_max_pack` in `ColumnMeta` |
| Rough result | `RSResult` with `All` (may skip RN row filter) | `FilterOpResult` without `All`; RN always filters |
| Index levels | Every stable DMFile | **L2 only** |
| Predicate parse | `FilterParser` + `DateQueryDomain` | tipb tree in `filter.rs` |
| TIMESTAMP TZ | `FilterParser::convertFieldWithTimezone` | RN `normalizeTimestampCompareDateTimeLiteralToUTC`; CSE has `TODO: timezone` |

Porting the DMFile protobuf / MergedSubFileInfo plan unchanged would not fit CSE's column-meta embedding and rolling-upgrade constraints.

## Terminology

| Term | Definition |
| --- | --- |
| ordinary min-max | Existing per-pack CSE `MinMaxIndex` over all non-NULL, non-deleted values |
| effective date range `E` | Persisted half-open interval `[lower, upper)` used when building a trim index |
| trim value | Value in `D ∩ E` that participates in trim min/max |
| trimmed value | Non-NULL, non-deleted value outside `E` |
| `pack_marks` | Trim payload's per-pack `UInt8`; bit 0 null, bit 1 low, bit 2 high, bit 3 has_value; no separate `has_value_marks` |
| trim-eligible | Predicate for which low/high trimmed values have uniform match semantics vs the stored `E` |
| trailer | Bytes appended after the ordinary min-max payload inside one compressed min-max blob |

## Goals

1. Restore L2 pack pruning for DATE / DATETIME / TIMESTAMP predicates when sparse outliers pollute ordinary min-max.
2. Preserve query correctness: never drop a matching pack via false `None`; never claim stronger certainty than CSE's `FilterOpResult` model allows.
3. Keep "new write / old read" safe without requiring old CSE binaries to understand new `ColumnMeta` fields.
4. Soft-fallback to ordinary min-max for old files, missing trailers, unknown trim versions, and ineligible predicates.
5. Bound write and metadata overhead; omit trim when a column has no trimmed value in the file.
6. Expose enough metrics (CSE + optional RN `ColumnarScanContext`) to validate selection, fallback, and pruning gain.

## Non-Goals

- Changing TiDB / TiFlash SQL semantics or temporal type semantics.
- DDL or user-visible index configuration of `E`.
- Writing trim for columnar L0/L1.
- Supporting `TIME` / duration / string / numeric columns in v1.
- Introducing CSE `All` / skipping RN row-level filters in v1.
- Actively rewriting historical columnar files; coverage grows via L2 compaction / major.
- Implementing full timezone conversion inside CSE in v1 (continue relying on RN TIMESTAMP normalization).
- Unifying DeltaMerge and CSE on-disk formats; only share semantics and packed bound constants.

## Correctness Foundation

The set algebra is identical to the DeltaMerge design.

Let `D` be the ordinary min-max value set of a pack and `E` the stored effective range. Trim represents `D_trim = D ∩ E`. For a non-NULL query domain `Q`:

- If `Q ⊆ E`, then `D ∩ Q = D_trim ∩ Q`, so trim may safely prove emptiness (`None`).
- Trim must not treat "all in-range values match" as pack-level certainty beyond CSE's model. CSE has no `All`, and RN always filters, so the dangerous DeltaMerge `All` path does not exist here.
- For one-sided ranges with the finite bound in `E`, directional trimmed flags are still required:

| Predicate | Low trimmed | High trimmed |
| --- | --- | --- |
| `col >= T` / `col > T` | Never matches | Always matches |
| `col <= T` / `col < T` | Always matches | Never matches |

Correction for CSE v1:

| Raw trim result | Condition | Final |
| --- | --- | --- |
| `None` | `trimmed_match_exists = false` | `None` |
| `None` | `trimmed_match_exists = true` | `Some` |
| `Some` / `Unknown` | any | unchanged |

Flags never upgrade certainty; they only prevent false negatives on one-sided predicates.

### AND / OR Composition

CSE already composes leaf results with `&` / `|`. Soundness requires **per-leaf** eligibility against the **stored** `E`, not a column-global choice:

```text
(t = 2020) OR (t = 2200)
```

`t = 2200` is not trim-eligible for `E = [1900, 2099-12)` and must use ordinary min-max. Otherwise an empty `D_trim` could return `None` and drop a matching pack from the `Or`.

Top-level `And` of one-sided bounds may optionally be normalized into a bounded range for clearer `Q ⊆ E` checks (mirroring DeltaMerge `DateRange`). Not rewriting under `Or` / `Not` remains the conservative rule.

## Design

### Overall Architecture

```text
Columnar L2 write
  -> ordinary MinMaxIndex (unchanged semantics)
  -> TrimMinMaxIndex builder for Date/DateTime/Timestamp(/NewDate)
  -> if any trimmed value in column:
       append trim trailer to ordinary payload
       compress once into compressed_min_max_pack
     else:
       write ordinary payload only (no trailer)

TiFlash RN
  -> normalize TIMESTAMP literals to UTC (existing)
  -> push tipb filters to CSE

CSE FilterOperator (per leaf, per pack)
  -> if trim-eligible for stored E and trailer present:
       raw = trim.MinMaxIndex.check_*
       apply None->Some correction from pack_marks
  -> else:
       ordinary MinMaxIndex (existing path)
  -> And/Or/Not compose FilterOpResult as today
```

### Effective Date Range

Default (format version 1):

```text
[1900-01-01 00:00:00, 2099-12-01 00:00:00)
```

Bounds are stored as little-endian packed `u64`, matching CSE's columnar temporal encoding (`decode_v2_u64` → 8-byte LE) and DeltaMerge `MyDate` / `MyDateTime::toPackedUInt` for the same calendar values.

- `DATE` / `NewDate`: packed date.
- `DATETIME` / `TIMESTAMP`: packed datetime (TIMESTAMP values are stored/compared as the UTC packed form already present in columnar data after RN normalization of literals).

Readers must use **persisted** bounds from the trailer, never the process default, when deciding eligibility.

### On-Disk Layout: Trim Trailer

`MinMaxIndex::parse` today consumes the ordinary prefix and does **not** require the buffer to end there. This enables a compatibility-safe trailer:

```text
[ ordinary MinMaxIndex payload ]          // identical to today
[ optional trim trailer:
    magic            u32   = 0x4D4D5254   // 'TRMM' LE
    format_version   u32   = 1
    lower_bound      u64   // packed LE
    upper_bound      u64   // packed LE, exclusive
    pack_count       u64
    trim_payload:
      ColumnBuffer(length = 2 * pack_count)  // min at 2i, max at 2i+1
      pack_marks[pack_count]                // bits 0..3 used; 4..7 must be 0
                                            // NO separate has_value_marks
]
```

Constraints:

1. Ordinary prefix byte layout and semantics are unchanged so old readers keep working. Ordinary continues to use separate `has_null_marks` + `has_value_marks`.
2. `format_version = 1` always means half-open `[lower, upper)` and the packed trim_payload layout above (has_value folded into `pack_marks`). Unknown versions → soft-fallback.
3. `pack_count` must equal the column's pack count.
4. `lower_bound < upper_bound`.
5. Reserved pack-mark bits (4..7) must be zero after load; violation is treated as corrupt trim (soft-fallback or hard-fail per policy below).
6. If the file has no trimmed value for the column, omit the trailer entirely (ordinary-only blob).
7. Ordinary and trim are published atomically in one compressed min-max blob with the immutable columnar file.
8. Trim serialization is **trim-specific**: it must not call ordinary `MinMaxIndex::write_to` unchanged, because that would emit a trailing `has_value_marks` array that this trailer format does not use.

**Soft fallback** (use ordinary / `Some`): missing trailer, bad magic, unknown version, undecodable/invalid bounds, `pack_count` mismatch, empty remaining buffer after ordinary parse.

**Hard fail** (existing columnar corruption policy): LZ4 / checksum failure on the compressed min-max pack itself. Do not reinterpret a corrupt compressed blob as "no index."

#### Rejected metadata alternatives

| Alternative | Why rejected for v1 |
| --- | --- |
| Put props/index into `column_props` | Current `ColumnMeta::parse` hard-skips only 4 bytes; non-zero props break old readers mid-`TableMeta` |
| New length-prefixed blob after `column_props` | Same sequential-parse breakage for old readers |
| Separate object-store file | Extra DFS object and GC coupling; unnecessary given embedded min-max |
| Replace ordinary with trim-only | Breaks ineligible predicates that need full `D` |

Fixing `column_props` length parsing is still desirable as cleanup, but must not be the trim compatibility vehicle in v1.

### Pack Marks

| Bit | Mask | Ordinary `has_null_marks` | Trim `pack_marks` |
| --- | --- | --- | --- |
| 0 | `0x01` | has_null | has_null |
| 1 | `0x02` | must be 0 | has_trimmed_low |
| 2 | `0x04` | must be 0 | has_trimmed_high |
| 3 | `0x08` | n/a (ordinary uses a separate `has_value_marks` byte) | has_value (in-range) |
| 4..7 | `0xf0` | must be 0 | must be 0; reserved |

Ordinary layout is unchanged: `has_null_marks` remain 0/1, and `has_value_marks` stay a separate per-pack array. Folding `has_value` into `pack_marks` applies **only** to the CSE trim trailer, saving one byte per pack versus carrying a DeltaMerge-style dual mark array, without hurting rough-check locality (the same `pack_marks` byte is already loaded for null/low/high correction).

Accessors:

```text
has_null         = (mark & 0x01) != 0
has_trimmed_low  = (mark & 0x02) != 0
has_trimmed_high = (mark & 0x04) != 0
has_value        = (mark & 0x08) != 0
```

Do not infer `has_value` from whether the min/max `ColumnBuffer` slots are null: the minmax buffer's `nullable` flag follows the column's nullability, so NotNull columns are not a reliable signal.

### In-Memory Model

Prefer composition for bounds/version, with a trim-specific mark representation:

```rust
struct TrimMinMaxIndex {
    // min/max values reuse ColumnBuffer layout (2 slots per pack).
    // pack_marks replaces ordinary has_null_marks + has_value_marks.
    min_max: ColumnBuffer,
    pack_marks: Vec<u8>,
    lower_bound: u64,
    upper_bound: u64,
    format_version: u32,
}

struct ColumnMeta {
    // existing fields...
    min_max: Option<MinMaxIndex>,
    trim_min_max: Option<TrimMinMaxIndex>, // parsed from trailer when present
}
```

Raw comparison helpers may still share logic with `MinMaxIndex::check_*`, but trim payload parse/write must be dedicated so `has_value` is read from / written to bit 3 of `pack_marks`.

`ColumnMeta::parse` flow:

1. Decompress `compressed_min_max_pack` if `min_max_idx_len > 0`.
2. Parse ordinary `MinMaxIndex` from the prefix.
3. If remaining bytes look like a valid trailer, parse `TrimMinMaxIndex`; else leave `trim_min_max = None`.

### Write Path

Extend `ColumnarColumnBuilder` so that when `need_min_max && is_supported_temporal(tp)`:

```text
for each row in pack:
  if deleted: continue
  if null:
    ordinary.has_null = true
    trim_pack_mark |= 0x01          // has_null
    continue
  ordinary.update_minmax(value)
  if lower <= value < upper:
    trim.update_minmax(value)
    trim_pack_mark |= 0x08          // has_value
  else if value < lower:
    trim_pack_mark |= 0x02          // has_trimmed_low
  else:
    trim_pack_mark |= 0x04          // has_trimmed_high

// after the pack loop:
// append trim min/max slots (or null placeholders when has_value is clear)
// append trim_pack_mark to pack_marks
```

Single traversal; do not scan the pack twice.

On `finish_min_max_pack`:

1. Serialize ordinary payload.
2. If the column-level "any trimmed" flag is set, append trailer with trim payload and bounds.
3. Compress once into `compressed_min_max_pack`.

No trim for handle / version columns, non-temporal types, L0/L1, or empty tables. Trim generation is not gated by a runtime switch; only the read path is configurable.

### Query-Domain Analysis (CSE)

Implement eligibility in `filter.rs` next to `CompareOperator`:

Supported trim-eligible forms (after tipb parse / optional AND normalize):

```text
col = T
col IN (T1, T2, ...)
col >= L AND col <= U   (and GT/LT variants)
col >= L / col > L
col <= U / col < U
```

Rules:

- Equality / IN / bounded range: require `Q ⊆ stored E`.
- One-sided: finite bound in `E`; pass predicate class into correction.
- First version: no trim for `NotEqual`, `NotIn`, `IsNull`, `Like`, casts, functions, or branches under `Or`/`Not` that are not independently eligible leaves.
- Per-leaf re-check at rough-check time against stored trailer bounds.

Optional v1 enhancement: flatten top-level `And` of opposite one-sided compares on the same column into a bounded range operator (DeltaMerge `DateRange` analogue) for clearer eligibility. Do not rewrite under `Or`.

`NotEqual` / `NotIn` continue to use ordinary min-max only.

### Per-File / Per-Column Selection

```text
if !trim_read_enabled                  -> ORDINARY
else if no trim trailer                -> ORDINARY
else if unsupported format_version     -> ORDINARY
else if props inconsistent             -> ORDINARY
else if !query_domain.is_trim_eligible -> ORDINARY
else                                   -> TRIM (+ correction)
```

L0/L1 columns have `min_max = None` today and already return `Some`; trim does not change that.

### NULL, Delete, MVCC

Align with ordinary CSE min-max:

- Deleted rows excluded from ordinary and trim; they do not set low/high bits.
- Non-deleted NULL sets null bit only; NULL is not a trimmed value and does not set `has_value`.
- `has_value` (bit 3) is set only when at least one in-range non-deleted value exists; otherwise raw trim checks treat the pack as empty for min/max.
- Pack MVCC stats (`PROP_KEY_PACK_MVCC_STATS`) remain independent of rough-check.

### Configuration and Switches

Read-side only. Add to `TableScanCtx` (and propagate into `FilterOperator` via `ParseCtx`):

```text
enable_trim_minmax: bool = false   // read path only
```

- **Write:** L2 columnar builds always maintain trim index state for supported temporal columns. If a column has any trimmed value in the file, the TRMM trailer is emitted during `finish_min_max_pack`. No build-time kill switch.
- **Read:** when `enable_trim_minmax` is false, ignore trim trailers and use ordinary min-max only. When true, apply per-predicate eligibility and trim rough-check correction.
- Default read is disabled; enable gradually on canary nodes like DeltaMerge `dt_enable_trim_minmax`.
- `E` is not runtime-configurable in v1; only persisted bounds matter for eligibility.

TiFlash may later plumb a session/global setting into `TableScanCtx::with_enable_trim_minmax`; v1 may enable via explicit scan context configuration.

### Observability

Extend CSE `ColumnarRuntimeStats` and, when useful, tipb `ColumnarScanContext` / C++ `ColumnarScanContext`:

```text
trim_minmax_selected_packs
trim_minmax_none_packs
trim_minmax_none_downgraded_packs
trim_minmax_fallback_count{reason}
trim_minmax_write_bytes
```

Fallback reasons at least: `disabled`, `no_trailer`, `unsupported_version`, `predicate_boundary_outside_range`, `unsupported_expression`, `metadata_mismatch`.

Existing `rough_check_{total,selected,skipped,unknown}_packs` remain the primary pruning counters.

## Compatibility and Invariants

### Query-Correctness Invariants

1. Trim must not make any matching pack disappear (`false None`).
2. Equality / IN / bounded ranges use trim only when `Q ⊆ stored E`.
3. One-sided ranges use trim only when the finite bound is in stored `E` and correction uses low/high marks.
4. Soft-fallback whenever logical selection checks fail.
5. RN row-level filters remain authoritative; CSE does not introduce `All`.
6. Trim mark accessors use only the defined bits: null = bit 0, low = bit 1, high = bit 2, has_value = bit 3; bits 4..7 must be zero.
7. Eligibility uses stored trailer bounds, not the process default.

### Disk-Format Compatibility

- Ordinary min-max prefix unchanged → old CSE reads new files.
- Old files without trailer → new CSE uses ordinary only.
- No change to `column_props` length contract in v1.
- No requirement to bump `ColumnarFileFooter.format_version` for trailers (footer version is currently informational). Prefer trailer magic/version for feature detection.
- L2 compaction / major naturally rewrites files; no forced backfill.

### Rolling Upgrade

1. Deploy CSE binaries that understand trim trailers (writes begin emitting them immediately on L2 rebuild).
2. Canary-enable **reads** via `TableScanCtx::with_enable_trim_minmax(true)` on a subset of nodes.
3. Verify old binaries still open new L2 files (ordinary prefix only).
4. Expand rollout; keep kill switch for one release cycle.

## Performance and Resource Overhead

Per temporal column per pack, the trim trailer stores approximately:

```text
min + max        16 bytes   // MyDateTime / packed u64
pack_marks        1 byte    // null | low | high | has_value
```

About **17 bytes/pack/column** uncompressed (one byte less than a DeltaMerge-style trim payload that keeps a separate `has_value_marks` array), then shares one LZ4 frame with ordinary min-max.

- Write: one extra bound compare per non-null non-deleted temporal value in the same traversal.
- Read: when trim-eligible, prefer trim checks; otherwise ordinary. Both indexes are already in memory after decompressing the single blob. Folding `has_value` into `pack_marks` avoids a second mark array touch on the hot path.
- Omit trailer when no trimmed value exists in the column.

## Phased Implementation

### Phase A: Format and Parse

- Define trailer magic / props / pack-mark helpers in CSE (`Null|TrimmedLow|TrimmedHigh|HasValue`, reserved mask `0xf0`).
- Implement trim-specific payload parse/write that stores only `ColumnBuffer + pack_marks` (no `has_value_marks`).
- Teach `ColumnMeta` parse/write to round-trip ordinary+trailer.
- Unit tests: old-prefix-only, trailer present, corrupt trailer soft-fallback, reserved bits, `has_value` bit round-trip.

### Phase B: Write Path

- Single-pass ordinary+trim update in `ColumnarColumnBuilder::finish_pack`.
- Gate on L2 + temporal types only (always build when applicable).
- Omit trailer when no trimmed value.
- Compaction/major path inherits via shared builder options.

### Phase C: Read Path / Eligibility

- Trim eligibility helpers for equality, IN, bounded, one-sided.
- Optional top-level AND range normalize.
- Per-leaf selection + `None→Some` correction in `FilterOperator::handle_comparison`.
- Metrics and integration tests with sentinel-contaminated packs.

### Phase D: Rollout

- Canary enable; compare rough-check skip ratios and result correctness vs disabled.
- Default-enable after soak; retain kill switch.

## Validation Strategy

### Unit Tests (CSE)

Pack shapes:

```text
all in E
all low / all high / both outliers
normal + 2100 sentinel
NULL only / NULL + normal + outlier
delete mark + normal + outlier
no valid values
```

Verify `pack_marks` bit combinations at least: `0x00`, `0x01` (null), `0x02` (low), `0x04` (high), `0x08` (has_value), `0x06` (low+high), `0x09` (null+has_value), `0x0e` / `0x0f` (value + outliers ± null). Reject nonzero bits 4..7.

Rough-check cases (mirror DeltaMerge where applicable):

```text
pack={2021, 2100}, query=[2020, 2022] -> Some
pack={2100}, query=[2020, 2022]       -> None
pack={2100}, query>=2020              -> Some (not None)
pack={1800}, query>=2020              -> None
pack={1800}, query<=2020              -> Some (not None)
pack={2100}, query<=2020              -> None
```

### Compatibility Tests

- New L2 with trailer opened by parser that only reads ordinary prefix.
- Old L2 without trailer opened by new reader.
- Invalid magic / version / bounds / pack_count → ordinary fallback.
- L0/L1 still return `Some` without panic.

### End-to-End (ENABLE_NEXT_GEN_COLUMNAR)

- Same SQL result sets with trim disabled / enabled.
- TIMESTAMP with RN timezone normalization + DATETIME/DATE calendar compares.
- Sentinel-contaminated dataset: skipped-pack ratio approaches no-outlier baseline for narrow recent ranges.

## Risks and Mitigations

1. **False `None` on one-sided ranges** — Persist low/high marks; mandatory `None→Some` correction; dedicated tests.
2. **Interpreting old trailers with new default `E`** — Eligibility uses persisted bounds only.
3. **OR leaf incorrectly using trim** — Per-predicate eligibility at use time.
4. **Trailing-byte parse ambiguity** — Strong magic + length/pack_count checks; on failure soft-fallback without touching ordinary result.
5. **Write CPU / size overhead** — Single pass; omit trailer when unused; default off.
6. **TIMESTAMP timezone drift** — Keep RN UTC normalization; document CSE `ParseCtx` timezone TODO as non-goal for v1; add TIMESTAMP DST tests through RN+CSE.
7. **Divergent DM vs CSE behavior** — Share `E`, packed constants, eligibility rules, and correction tables in design; accept different on-disk containers.

## Alternatives

1. **Only use trim as an extra `None` gate while always loading ordinary** — Safer but doubles logic and cannot help when ordinary max is already polluted for complementary checks; rejected in favor of replace-when-eligible (same as DeltaMerge).
2. **Introduce CSE `All` to skip RN filters** — Large semantic change across RN/CSE; out of scope.
3. **Build trim on L0/L1** — Ordinary min-max is L2-only today; expanding levels is a separate project.
4. **Implement eligibility only in TiFlash C++** — CSE owns rough-check; pushing selection to RN would require new tipb annotations and still need CSE correction. Rejected for v1.
5. **Keep a separate `has_value_marks` array in the trim trailer (DeltaMerge-style)** — Works, but wastes one byte per pack and an extra array touch. CSE trim already has a dedicated trailer format, so folding `has_value` into `pack_marks` bit 3 is preferred. Ordinary min-max keeps its dual mark arrays for compatibility.
6. **Infer `has_value` from null min/max slots** — Unsafe for NotNull columns whose minmax `ColumnBuffer` may not treat empty packs as null the same way; rejected in favor of an explicit bit.

## Established Design Boundaries

- `E = [1900-01-01, 2099-12-01)` half-open; persisted per column trailer.
- Trim trailer appended after ordinary min-max payload inside `compressed_min_max_pack`.
- Ordinary prefix byte-compatible with existing CSE readers; ordinary still uses separate `has_null_marks` + `has_value_marks`.
- Trim `pack_marks`: null / low / high / has_value bits; bits 4..7 zero; no separate trim `has_value_marks`.
- L2-only; temporal types only; read-side `enable_trim_minmax` switch (default off).
- CSE `FilterOpResult` model unchanged: no `All`; only `None→Some` trim correction.
- Per-leaf eligibility; soft-fallback on logical meta problems.
- No historical backfill; natural L2 rewrite increases coverage.

## Open Questions

None that block the v1 shape above. Follow-ups that may be tracked outside this doc:

1. Whether to plumb TiFlash `dt_enable_trim_minmax` into `TableScanCtx::with_enable_trim_minmax` automatically.
2. Whether to later fix `column_props` length parsing as independent cleanup and migrate trailers into first-class column props in a future format version.
3. Whether RN should surface trim-specific counters in `EXPLAIN ANALYZE` beyond existing rough-check pack stats.
