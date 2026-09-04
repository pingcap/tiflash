# Trim Min-Max Index for DATE / DATETIME / TIMESTAMP in Next-Gen Columnar (CSE)

- Status: Accepted
- Date: 2026-08-31
- Last updated: 2026-09-04
- Related: [2026-07-14-trim-minmax-for-date-types.md](./2026-07-14-trim-minmax-for-date-types.md) (DeltaMerge / DMFile)
- Primary change surface: `contrib/cloud-storage-engine` (`kvengine` columnar)

## Summary

This document ports the DeltaMerge `trim_minmax` optimization to TiFlash **next-gen columnar** mode (`ENABLE_NEXT_GEN_COLUMNAR=ON`), where pack-level min-max and rough-check live in Cloud Storage Engine (CSE) rather than in DMFile.

The problem is identical: sparse sentinel timestamps such as `2100-01-01 00:00:00` inflate ordinary pack min-max and destroy pruning for narrow recent-time predicates. The correctness foundation (effective interval `E`, trimmed low/high marks, per-predicate eligibility, conservative `None` correction) is intentionally the same as the DeltaMerge design.

The CSE design adopts these decisions:

1. Reuse the same default half-open effective range `E = [1900-01-01 00:00:00, 2099-12-01 00:00:00)` and the same packed `u64` temporal encoding already used by CSE columnar values.
2. Keep the ordinary L2 `MinMaxIndex` unchanged. Build an optional **trim** min-max for supported temporal user columns during the same pack traversal.
3. Persist trim as an **optional trailer** appended after the ordinary min-max payload inside the existing `ColumnMeta.compressed_min_max_pack`. Old readers parse only the ordinary prefix and ignore trailing bytes; they do not need a new `ColumnMeta` field layout.
4. Encode per-pack flags in a single unified `pack_marks` byte (same layout in memory and in the TRMM trailer): bit 0 = null, bit 1 = ordinary has_value, bit 2 = trimmed low, bit 3 = trimmed high, bit 4 = trim has_value (in-range); bits 5..7 reserved (`0xE0`). The ordinary **prefix** still expands null / ordinary-has_value into separate legacy byte arrays for MinMax compatibility; the trailer stores only trim values + this unified `pack_marks` array (no separate trailer `has_value_marks`).
5. Perform trim eligibility and rough-check selection entirely inside CSE `FilterOperator` (Rust). TiFlash RN continues to push tipb filters and apply TIMESTAMP literal UTC normalization; it does not load CSE min-max bytes.
6. CSE rough-check results remain `None` / `Some` / `Unknown` and only skip pack I/O. There is **no** `All` that skips row-level filtering on the RN. Therefore CSE only needs `None → Some` correction when a trimmed value must match; the DeltaMerge `All → Some` path is not required for v1.
7. Write trim only on **columnar L2** (same gate as ordinary min-max). L0/L1, non-temporal columns, disabled config, and ineligible predicates always fall back to ordinary min-max (or `Some` when no index exists).
8. Gate trim **reads** via `TableScanCtx::enable_trim_minmax` (plumbed from TiFlash `dt_enable_trim_minmax`). L2 writes always build trim indexes for supported temporal columns when outliers exist.
9. When trim reads are enabled, flatten top-level `And` and merge same-column one-sided temporal compares into a CSE **`DateRange`** leaf (DeltaMerge `normalizeTemporalRangesForTrim` analogue), so bounded ranges use `EqualityOrInOrBounded` correction instead of composing independent one-sided corrections.

## Background

### Current CSE Columnar Min-Max

Relevant code:

- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/columnar.rs` — `MinMaxIndex`, `ColumnMeta`, `TableMeta`
- `.../columnar/builder.rs` — `ColumnarColumnBuilder::finish_pack` / `compute_min_max` / `finish_min_max_pack`
- `.../columnar/temporal_minmax.rs` — `TemporalMinMaxIndex`, trailer parse/write, trim rough-check
- `.../columnar/filter.rs` — tipb → `FilterOperator` → `rough_check`
- `.../columnar/reader.rs` — skip packs with `FilterOpResult::None`

Write path (L2 only, `level == MAX_COLUMNAR_LEVEL`):

```text
ColumnarTableBuilder.append
  -> ColumnarColumnBuilder::finish_pack(del_marks)
       -> has_null = any (null && !deleted)
       -> compute_min_max / compute_temporal_min_max
       -> MinMaxIndex or TemporalMinMaxIndex push min/max + marks
  -> finish_min_max_pack
       -> ordinary prefix (+ optional TRMM trailer) -> compress_pack
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
CSE TableScanCtx::to_filter_operator
  -> parse tipb And(...)
  -> if enable_trim_minmax: normalize_temporal_ranges_for_trim
CSE FilterOperator::rough_check(TableMeta)
  -> per pack: ordinary / temporal / DateRange checks -> FilterOpResult::{None,Some,Unknown}
ColumnReader::load_pack
  -> None: skip I/O
  -> Some/Unknown: load pack
TiFlash RN
  -> always applies row-level filters (CSE does not claim All)
```

### Problem Scenario

Same as the DeltaMerge design: with pack size 8,192 and one `2100-01-01` sentinel per 10,000 rows, about 56% of packs have `max = 2100`, so narrow recent-range predicates lose most ordinary min-max pruning power.

Independently evaluating `t >= L` and `t <= U` as one-sided trim leaves is not enough for BETWEEN-shaped queries: each leaf may apply directional `None→Some` correction, and their conjunction keeps packs that a true bounded range (`Q ⊆ E`) can prove empty. DeltaMerge avoids this by normalizing opposite one-sided bounds into `DateRange` with `EqualityOrInOrBounded`; CSE must do the same on the rough-check tree.

### Why a Separate Columnar Design

| Dimension | DeltaMerge | CSE columnar |
| --- | --- | --- |
| Owner of index I/O | TiFlash C++ `DMFilePackFilter` | CSE Rust `FilterOperator` / `ColumnReader` |
| Storage | Separate `.idx` / `.trim.idx` + `ColumnStat` protobuf | Embedded `compressed_min_max_pack` in `ColumnMeta` |
| Rough result | `RSResult` with `All` (may skip RN row filter) | `FilterOpResult` without `All`; RN always filters |
| Index levels | Every stable DMFile | **L2 only** |
| Predicate parse | `FilterParser` + `DateQueryDomain` | tipb tree in `filter.rs` (+ `DateRange` normalize) |
| TIMESTAMP TZ | `FilterParser::convertFieldWithTimezone` | RN `normalizeTimestampCompareDateTimeLiteralToUTC`; CSE has `TODO: timezone` |

Porting the DMFile protobuf / MergedSubFileInfo plan unchanged would not fit CSE's column-meta embedding and rolling-upgrade constraints.

## Terminology

| Term | Definition |
| --- | --- |
| ordinary min-max | Existing per-pack CSE `MinMaxIndex` over all non-NULL, non-deleted values |
| effective date range `E` | Persisted half-open interval `[lower, upper)` used when building a trim index |
| trim value | Value in `D ∩ E` that participates in trim min/max |
| trimmed value | Non-NULL, non-deleted value outside `E` |
| `pack_marks` | Unified per-pack `UInt8` (memory + TRMM trailer): null / ordinary_has_value / trimmed_low / trimmed_high / trim_has_value; bits 5..7 reserved |
| trim-eligible | Predicate for which low/high trimmed values have uniform match semantics vs the stored `E` |
| trailer | Bytes appended after the ordinary min-max payload inside one compressed min-max blob |
| `TemporalMinMaxIndex` | In-memory ordinary + optional trim view for a temporal column (parsed from ordinary prefix + TRMM trailer) |
| `DateRange` | Rough-check-only leaf for a temporal interval or one-sided bound (`FilterType::DateRange`); DM alias `DM::DateRange` |
| top-level And flatten | Collapse nested `And` above `Or` / `Not` / compares before merging temporal bounds |

## Goals

1. Restore L2 pack pruning for DATE / DATETIME / TIMESTAMP predicates when sparse outliers pollute ordinary min-max.
2. Preserve query correctness: never drop a matching pack via false `None`; never claim stronger certainty than CSE's `FilterOpResult` model allows.
3. Keep "new write / old read" safe without requiring old CSE binaries to understand new `ColumnMeta` fields.
4. Soft-fallback to ordinary min-max for old files, missing trailers, unknown trim versions, and ineligible predicates.
5. Bound write and metadata overhead; omit trim when a column has no trimmed value in the file.
6. Expose enough metrics (CSE + optional RN `ColumnarScanContext`) to validate selection, fallback, and pruning gain.
7. For BETWEEN-shaped tipb (`col >= L AND col <= U` and GT/LT variants), prune packs that only survive via independent one-sided outlier correction.

## Non-Goals

- Changing TiDB / TiFlash SQL semantics or temporal type semantics.
- DDL or user-visible index configuration of `E`.
- Writing trim for columnar L0/L1.
- Supporting `TIME` / duration / string / numeric columns in v1.
- Introducing CSE `All` / skipping RN row-level filters in v1.
- Actively rewriting historical columnar files; coverage grows via L2 compaction / major.
- Implementing full timezone conversion inside CSE in v1 (continue relying on RN TIMESTAMP normalization).
- Unifying DeltaMerge and CSE on-disk formats; only share semantics and packed bound constants.
- Rewriting ranges under `Or` / `Not`, or inventing a general predicate simplifier beyond DateRange normalize.

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

CSE composes leaf results with `&` / `|`. Soundness requires **per-leaf** eligibility against the **stored** `E`, not a column-global choice:

```text
(t = 2020) OR (t = 2200)
```

`t = 2200` is not trim-eligible for `E = [1900, 2099-12)` and must use ordinary min-max. Otherwise an empty `D_trim` could return `None` and drop a matching pack from the `Or`.

**Bounded ranges must not stay as independent one-sided leaves.** For a pack with in-range history plus a high sentinel and query `L <= t <= U` with `Q ⊆ E`:

| Leaf | Trim raw | Correction | Result |
| --- | --- | --- |
| `t >= L` | often `None` | `TRIMMED_HIGH` → `Some` | keep |
| `t <= U` | often `Some` | n/a | keep |
| `And` of the two | | | **keep** (too permissive) |

No row may satisfy both bounds, yet the conjunction retains the pack. Therefore, when trim reads are enabled, top-level `And` of opposite one-sided temporal compares on the same column **must** be normalized into a `DateRange` leaf with `EqualityOrInOrBounded` (outliers never force `None→Some`). Not rewriting under `Or` / `Not` remains the conservative rule (same as DeltaMerge).

## Design

### Overall Architecture

```text
Columnar L2 write
  -> ordinary MinMaxIndex (unchanged semantics)
  -> TemporalMinMaxIndex builder for Date/DateTime/Timestamp(/NewDate)
  -> if any trimmed value in column:
       append trim trailer to ordinary payload
       compress once into compressed_min_max_pack
     else:
       write ordinary payload only (no trailer)

TiFlash RN
  -> normalize TIMESTAMP literals to UTC (existing)
  -> plumb dt_enable_trim_minmax into TableScanCtx
  -> push tipb filters to CSE

CSE FilterOperator
  -> parse tipb
  -> if enable_trim_minmax: normalize_temporal_ranges_for_trim
       (flatten top-level And; merge Ge/Gt/Le/Lt per col into DateRange)
  -> per leaf / DateRange, per pack:
       if trim-eligible for stored E and trailer present:
            raw trim check (+ None->Some correction by predicate class)
       else:
            ordinary min-max
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

`compressed_min_max_pack` for a temporal column is one LZ4 blob. After decompress, CSE writes / parses:

```text
[ ordinary MinMax prefix ]                // legacy-compatible
  ColumnBuffer(length = 2 * num_packs)    // ordinary min at 2i, max at 2i+1
  has_null_marks[num_packs]               // 0/1 bytes (expanded from pack_marks NULL)
  has_value_marks[num_packs]              // 0/1 bytes (expanded from ORDINARY_HAS_VALUE)

[ optional TRMM trailer ]                 // only if the column has any trimmed outlier
  magic            u32 LE = 0x4D4D5254    // 'TRMM'
  format_version   u32 LE = 1
  lower_bound      u64 LE                 // packed; half-open E start
  upper_bound      u64 LE                 // packed; half-open E end (exclusive)
  pack_count       u64 LE                 // must equal num_packs
  trim_payload:
    ColumnBuffer(length = 2 * num_packs)  // trim min at 2i, max at 2i+1
    pack_marks[pack_count]                // unified marks (see Pack Marks);
                                          // bits 5..7 must be 0
```

This matches `TemporalMinMaxIndex::{write_ordinary_prefix_to, write_trailer_to, parse_ordinary_prefix, try_parse_trailer_into}`.

Constraints:

1. Ordinary prefix byte layout and semantics are unchanged so old readers keep working. Temporal writers expand unified `pack_marks` into separate `has_null_marks` + `has_value_marks` for that prefix only.
2. `format_version = 1` means half-open `[lower, upper)` and the trim_payload layout above. Unknown versions → soft-ignore trailer (keep ordinary; leave trim disabled). Bumping TRMM version must not change ordinary-prefix layout (`MAINTAINER_GUIDE` §17.3).
3. Trailer `pack_count` must equal the column's pack count (and the trim `ColumnBuffer` length / 2).
4. `lower_bound < upper_bound`.
5. Reserved pack-mark bits 5..7 (`RESERVED_MASK = 0xE0`) must be zero in the trailer; non-zero is a hard `InvalidPackMarks` parse error for a claimed v1 trailer.
6. If the file has no trimmed value for the column, omit the trailer entirely (ordinary-only blob).
7. Ordinary and trim are published atomically in one compressed min-max blob with the immutable columnar file.
8. Trailer `pack_marks` are authoritative after a successful parse: they replace any ordinary-only bits reconstructed from the prefix (same unified layout as in memory).
9. Trim serialization is **trim-specific**: do not call ordinary `MinMaxIndex::write_to` for the trailer, because that would emit another legacy `has_value_marks` array the trailer format does not use.

**Soft fallback** (use ordinary / `Some`, trim disabled): missing trailer, bad/absent magic, unknown `format_version`, empty remaining buffer after ordinary parse.

**Hard fail** (existing columnar corruption policy): LZ4 / checksum failure on the compressed min-max pack itself; corrupt claimed-v1 trailers (`TooShort`, `InvalidBounds`, `PackCountMismatch`, `InvalidPackMarks`). Do not reinterpret a corrupt compressed blob as "no index."

#### Rejected metadata alternatives

| Alternative | Why rejected for v1 |
| --- | --- |
| Put props/index into `column_props` | Current `ColumnMeta::parse` hard-skips only 4 bytes; non-zero props break old readers mid-`TableMeta` |
| New length-prefixed blob after `column_props` | Same sequential-parse breakage for old readers |
| Separate object-store file | Extra DFS object and GC coupling; unnecessary given embedded min-max |
| Replace ordinary with trim-only | Breaks ineligible predicates that need full `D` |

Fixing `column_props` length parsing is still desirable as cleanup, but must not be the trim compatibility vehicle in v1.

### Pack Marks

Unified `pack_marks[i]` (module `pack_mark` in `temporal_minmax.rs`), identical in memory and in the TRMM trailer:

| Bit | Mask | Name | Meaning |
| --- | --- | --- | --- |
| 0 | `0x01` | `NULL` | pack has at least one non-deleted NULL |
| 1 | `0x02` | `ORDINARY_HAS_VALUE` | pack has at least one non-null value (including outliers) |
| 2 | `0x04` | `TRIMMED_LOW` | pack has at least one non-null value `< lower_bound` |
| 3 | `0x08` | `TRIMMED_HIGH` | pack has at least one non-null value `>= upper_bound` |
| 4 | `0x10` | `TRIM_HAS_VALUE` | pack has at least one non-null value in `[lower_bound, upper_bound)` |
| 5..7 | `0xE0` | reserved | must be 0; non-zero → `InvalidPackMarks` |

On the ordinary **prefix** only, bits 0 and 1 are expanded into separate legacy arrays (`has_null_marks` / `has_value_marks` as 0/1 bytes) so old MinMax readers stay compatible. The trailer does **not** repeat those arrays; it stores the unified `pack_marks` once next to the trim `ColumnBuffer`.

Accessors:

```text
has_null              = (mark & 0x01) != 0   // NULL
ordinary_has_value    = (mark & 0x02) != 0   // ORDINARY_HAS_VALUE
has_trimmed_low       = (mark & 0x04) != 0   // TRIMMED_LOW
has_trimmed_high      = (mark & 0x08) != 0   // TRIMMED_HIGH
has_value (trim)      = (mark & 0x10) != 0   // TRIM_HAS_VALUE
```

Do not infer trim `has_value` from whether the min/max `ColumnBuffer` slots are null: the minmax buffer's `nullable` flag follows the column's nullability, so NotNull columns are not a reliable signal.

### In-Memory Model

Prefer a unified temporal index that holds ordinary + optional trim views:

```rust
struct TemporalMinMaxIndex {
    ordinary_values: ColumnBuffer, // 2 slots per pack
    trim_values: ColumnBuffer,     // 2 slots per pack (meaningful when trailer present)
    pack_marks: Vec<u8>,           // NULL | ORDINARY_HAS_VALUE | LOW | HIGH | TRIM_HAS_VALUE
    lower_bound: u64,
    upper_bound: u64,
    format_version: u32,
    // has_trim_trailer / parse state as needed
}

struct ColumnMeta {
    // existing fields...
    min_max: Option<MinMaxIndex>,                 // non-temporal (or legacy ordinary-only)
    temporal_min_max: Option<TemporalMinMaxIndex>, // temporal columns; trailer optional
}
```

Raw comparison helpers may still share logic with `MinMaxIndex::check_*`, but trim payload parse/write must be dedicated so trim `has_value` is read from / written to bit 4 (`TRIM_HAS_VALUE`) of `pack_marks`.

`ColumnMeta::parse` flow:

1. Decompress `compressed_min_max_pack` if `min_max_idx_len > 0`.
2. For temporal columns, parse ordinary prefix into `TemporalMinMaxIndex`; if remaining bytes look like a valid trailer, attach the trim view (replacing `pack_marks` with trailer marks); else leave trailer absent (`has_trim_trailer() == false`).
3. For non-temporal columns, parse ordinary `MinMaxIndex` as today.

### Write Path

Extend `ColumnarColumnBuilder` so that when `need_min_max && is_supported_temporal(tp)`:

```text
for each row in pack:
  if deleted: continue
  if null:
    mark |= NULL                    // 0x01
    continue
  mark |= ORDINARY_HAS_VALUE        // 0x02
  ordinary.update_minmax(value)
  if lower <= value < upper:
    trim.update_minmax(value)
    mark |= TRIM_HAS_VALUE          // 0x10
  else if value < lower:
    mark |= TRIMMED_LOW             // 0x04
  else:
    mark |= TRIMMED_HIGH            // 0x08

// after the pack loop:
// append ordinary / trim min-max slots (null placeholders when no value)
// push unified mark into pack_marks
```

Single traversal; do not scan the pack twice.

On `finish_min_max_pack`:

1. Serialize ordinary payload.
2. If the column-level "any trimmed" flag is set, append trailer with trim payload and bounds.
3. Compress once into `compressed_min_max_pack`.

No trim for handle / version columns, non-temporal types, L0/L1, or empty tables. Trim generation is not gated by a runtime switch; only the read path is configurable.

### Query-Domain Analysis (CSE)

Implement eligibility in `filter.rs` next to `CompareOperator` / `DateRangeCompare`.

Supported trim-eligible forms (after tipb parse and DateRange normalize):

```text
col = T
col IN (T1, T2, ...)
col >= L AND col <= U   (and GT/LT variants)  // as DateRange, EqualityOrInOrBounded
col >= L / col > L                              // DateRange or one-sided leaf, LowerBounded
col <= U / col < U                              // DateRange or one-sided leaf, UpperBounded
```

Rules:

- Equality / IN / bounded range: require `Q ⊆ stored E`.
- One-sided: finite bound in `E`; pass predicate class into correction.
- No trim for `NotEqual`, `NotIn`, `IsNull`, `Like`, casts, functions, or branches under `Or`/`Not` that are not independently eligible leaves.
- Per-leaf / DateRange re-check at rough-check time against stored trailer bounds.

`NotEqual` / `NotIn` continue to use ordinary min-max only.

DateRange normalize details are Phase D.

### DateRange Normalize (Read Path)

Mirror DeltaMerge `normalizeTemporalRangesForTrim`, implemented in CSE only.

**When:** end of `TableScanCtx::to_filter_operator`, **only if** `enable_trim_minmax` is true. Trim off → no rewrite (ordinary path unchanged).

**Representation:**

```text
FilterType::DateRange
DateRangeCompare {
  col_id,
  lower / upper: Option<packed LE bytes>,
  lower_inclusive / upper_inclusive: bool,
  predicate_class: TrimPredicateClass,  // derived at normalize time
}
```

Bound mapping (same as DM):

| Leaf | BoundSide |
| --- | --- |
| `GreaterEqual` | lower inclusive |
| `Greater` | lower exclusive |
| `LessEqual` | upper inclusive |
| `Less` | upper exclusive |

Multiple bounds on the same side: keep the **stronger** bound (larger lower / smaller upper; on tie prefer exclusive).

**Algorithm (`normalize_temporal_ranges_for_trim`):**

```text
1. Flatten top-level And into leaves (iterative; preserve left-to-right order).
2. For each leaf:
   - If Ge/Gt/Le/Lt on a supported temporal column: accumulate BoundAccumulator[col_id].
   - Else: keep as-is (Equal, In, Or, Not, non-temporal, Unsupported, …).
3. For each accumulator:
   - On decode failure or empty bounds: append originals unchanged.
   - Else emit DateRange with class:
       lower && upper -> EqualityOrInOrBounded
       lower only     -> LowerBounded
       upper only     -> UpperBounded
4. Rebuild And(kept) (or single leaf).
```

Hard rules:

- Do **not** recurse into `Or` / `Not` when flattening.
- Do **not** merge Equal / In into DateRange.
- Temporal gate uses `is_supported_temporal_type` (Duration stays out, matching trim write support).
- Resolve column type from `ParseCtx.columns` / `to_filter_operator` column list by `col_id`.

**Rough-check:** evaluate lower and upper jointly against trim (or ordinary) min/max with correct inclusivity; apply `None→Some` correction using the DateRange's `predicate_class`. Bounded ranges therefore do not keep packs solely because of high/low outliers.

**Fallback:** undecodable bounds → keep original leaves; no trailer / ineligible endpoints → ordinary joint range check; nested And under Or → untouched (may forgo benefit; never wrong).

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
- Non-deleted NULL sets `NULL` only; NULL is not a trimmed value and does not set `ORDINARY_HAS_VALUE` / `TRIM_HAS_VALUE`.
- `TRIM_HAS_VALUE` (bit 4) is set only when at least one in-range non-deleted value exists; otherwise raw trim checks treat the pack as empty for min/max.
- Pack MVCC stats (`PROP_KEY_PACK_MVCC_STATS`) remain independent of rough-check.

### Configuration and Switches

Read-side only. `TableScanCtx` (propagated into `FilterOperator` via `ParseCtx`):

```text
enable_trim_minmax: bool = false   // read path only
```

- **Write:** L2 columnar builds always maintain trim index state for supported temporal columns. If a column has any trimmed value in the file, the TRMM trailer is emitted during `finish_min_max_pack`. No build-time kill switch.
- **Read:** when `enable_trim_minmax` is false, skip DateRange normalize, ignore trim trailers, and use ordinary min-max only. When true, normalize top-level And into DateRange where applicable, then apply per-predicate eligibility and trim rough-check correction.
- Default read is disabled; enable gradually on canary nodes via TiFlash `dt_enable_trim_minmax` plumbed into `TableScanCtx::with_enable_trim_minmax`.
- `E` is not runtime-configurable in v1; only persisted bounds matter for eligibility.
- No separate config key for DateRange normalize; it shares the same kill switch.

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

Existing `rough_check_{total,selected,skipped,unknown}_packs` remain the primary pruning counters for DateRange benefit as well. Optional debug logging of DateRange rewrite count is sufficient for v1; no mandatory new metric for normalize itself.

## Compatibility and Invariants

### Query-Correctness Invariants

1. Trim must not make any matching pack disappear (`false None`).
2. Equality / IN / bounded ranges use trim only when `Q ⊆ stored E`.
3. One-sided ranges use trim only when the finite bound is in stored `E` and correction uses low/high marks.
4. Soft-fallback whenever logical selection checks fail.
5. RN row-level filters remain authoritative; CSE does not introduce `All`.
6. Trim mark accessors use only the defined bits: null = bit 0, ordinary_has_value = bit 1, low = bit 2, high = bit 3, trim_has_value = bit 4; bits 5..7 (`0xE0`) must be zero.
7. Eligibility uses stored trailer bounds, not the process default.
8. `Or` / `Not` never gain DateRange rewrite; Equal / In are not merged into DateRange.
9. With trim disabled, the operator tree has no DateRange nodes and rough-check matches ordinary-only behavior for the same tipb.

### Disk-Format Compatibility

- Ordinary min-max prefix unchanged → old CSE reads new files.
- Old files without trailer → new CSE uses ordinary only.
- No change to `column_props` length contract in v1.
- No requirement to bump `ColumnarFileFooter.format_version` for trailers (footer version is currently informational). Prefer trailer magic/version for feature detection.
- L2 compaction / major naturally rewrites files; no forced backfill.
- DateRange normalize is read-path only; no disk migration.

### Rolling Upgrade

1. Deploy CSE binaries that understand trim trailers (writes begin emitting them immediately on L2 rebuild).
2. Deploy TiFlash / hub that plumbs `dt_enable_trim_minmax` and performs DateRange normalize when enabled.
3. Canary-enable **reads** via `dt_enable_trim_minmax` on a subset of nodes.
4. Verify old binaries still open new L2 files (ordinary prefix only).
5. Expand rollout; keep kill switch for one release cycle.

## Performance and Resource Overhead

Per temporal column per pack, the trim trailer stores approximately:

```text
min + max        16 bytes   // MyDateTime / packed u64
pack_marks        1 byte    // NULL | ORDINARY_HAS_VALUE | LOW | HIGH | TRIM_HAS_VALUE
```

About **17 bytes/pack/column** uncompressed for the trim trailer payload (ordinary prefix still has its own values + dual mark arrays), then shares one LZ4 frame with ordinary min-max. Versus a DeltaMerge-style trim that keeps a separate trim `has_value_marks` array, CSE folds trim has_value into the same unified byte (and also carries ordinary has_value in that byte so trailer marks can replace the in-memory view).

- Write: one extra bound compare per non-null non-deleted temporal value in the same traversal.
- Read: when trim-eligible, prefer trim checks; otherwise ordinary. Both indexes are already in memory after decompressing the single blob. Unified `pack_marks` avoids a second mark-array touch on the hot path.
- Normalize: O(n) flatten + per-`col_id` bound merge; negligible vs pack I/O.
- Omit trailer when no trimmed value exists in the column.

## Phased Implementation

### Phase A: Format and Parse

- Define trailer magic / pack-mark helpers in CSE (`NULL|ORDINARY_HAS_VALUE|TRIMMED_LOW|TRIMMED_HIGH|TRIM_HAS_VALUE`, reserved mask `0xE0`).
- Implement trim-specific payload parse/write that stores `ColumnBuffer + unified pack_marks` (no separate trailer `has_value_marks`).
- Teach `ColumnMeta` parse/write to round-trip ordinary+trailer via `TemporalMinMaxIndex`.
- Unit tests: old-prefix-only, trailer present, corrupt v1 trailer hard-fail, unknown version soft-ignore, reserved bits, `TRIM_HAS_VALUE` round-trip.

### Phase B: Write Path

- Single-pass ordinary+trim update in `ColumnarColumnBuilder::finish_pack`.
- Gate on L2 + temporal types only (always build when applicable).
- Omit trailer when no trimmed value.
- Compaction/major path inherits via shared builder options.

### Phase C: Read Path / Eligibility (leaf)

- Trim eligibility helpers for equality, IN, one-sided compares.
- Per-leaf selection + `None→Some` correction in `FilterOperator`.
- Plumb TiFlash `dt_enable_trim_minmax` into `TableScanCtx::with_enable_trim_minmax`.
- Metrics and integration tests with sentinel-contaminated packs for equality / one-sided shapes.

Phase C alone is **not** sufficient for BETWEEN-shaped tipb; that requires Phase D.

### Phase D: DateRange AND Normalize

- Add `FilterType::DateRange` and `DateRangeCompare`.
- Implement `normalize_temporal_ranges_for_trim` at the end of `to_filter_operator` when trim reads are enabled.
- Joint range rough-check + correction by merged `TrimPredicateClass`.
- Unit tests:
  - flatten nested top-level `And`; no rewrite under `Or` / `Not`
  - stronger-bound merge; inclusive/exclusive endpoints
  - bounded range: sentinel-only / inflated packs → `None` with trim ON (not kept via one-sided correction)
  - one-sided DateRange / leaf still applies directional correction
  - trim OFF: no DateRange nodes
- Update any pre-normalize BETWEEN unit expectations that encoded independent-leaf correction behavior.

### Phase E: Rollout

- Canary enable; compare rough-check skip ratios and result correctness vs disabled, including BETWEEN-style workloads.
- Default-enable after soak; retain kill switch (`dt_enable_trim_minmax`).

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

Verify `pack_marks` bit combinations at least: `0x00`, `0x01` (NULL), `0x02` (ORDINARY_HAS_VALUE), `0x04` (TRIMMED_LOW), `0x08` (TRIMMED_HIGH), `0x10` (TRIM_HAS_VALUE), `0x0c` (low+high), `0x13` (null+ordinary+trim has_value), `0x1e` / `0x1f` (ordinary+trim value + outliers ± null). Reject nonzero bits 5..7 (`0xE0`).

Rough-check cases (mirror DeltaMerge where applicable):

```text
pack={2021, 2100}, query=[2020, 2022] -> Some
pack={2100}, query=[2020, 2022]       -> None
pack={2100}, query>=2020              -> Some (not None)
pack={1800}, query>=2020              -> None
pack={1800}, query<=2020              -> Some (not None)
pack={2100}, query<=2020              -> None
```

DateRange / normalize cases:

```text
And(Ge, Le) on same temporal col     -> one DateRange leaf
And(And(Ge, Le), EqOther)            -> flatten; DateRange + Eq
Or(And(Ge, Le), ...)                 -> inner And not rewritten
pack trim max < L, has TRIMMED_HIGH,
  query [L,U] ⊆ E                    -> None (bounded; not Some via >= correction)
trim OFF                             -> no DateRange in tree
```

### Compatibility Tests

- New L2 with trailer opened by parser that only reads ordinary prefix.
- Old L2 without trailer opened by new reader.
- Invalid magic / version / bounds / pack_count → ordinary fallback.
- L0/L1 still return `Some` without panic.

### End-to-End (ENABLE_NEXT_GEN_COLUMNAR)

- Same SQL result sets with trim disabled / enabled.
- TIMESTAMP with RN timezone normalization + DATETIME/DATE calendar compares.
- Sentinel-contaminated dataset: skipped-pack ratio for narrow recent **ranges** approaches no-outlier baseline (BETWEEN / Ge+Le), not only equality.

## Risks and Mitigations

1. **False `None` on one-sided ranges** — Persist low/high marks; mandatory `None→Some` correction; dedicated tests.
2. **Interpreting old trailers with new default `E`** — Eligibility uses persisted bounds only.
3. **OR leaf incorrectly using trim** — Per-predicate eligibility at use time; no DateRange rewrite under `Or`.
4. **Trailing-byte parse ambiguity** — Strong magic + length/pack_count checks; on failure soft-fallback without touching ordinary result.
5. **Write CPU / size overhead** — Single pass; omit trailer when unused; default off.
6. **TIMESTAMP timezone drift** — Keep RN UTC normalization; document CSE `ParseCtx` timezone TODO as non-goal for v1; add TIMESTAMP DST tests through RN+CSE.
7. **Divergent DM vs CSE behavior** — Share `E`, packed constants, eligibility rules, DateRange normalize rules, and correction tables in design; accept different on-disk containers.
8. **Incorrect exclusive-bound handling in DateRange** — Dedicated GT/LT unit cases; packed-u64 endpoint compares shared with existing helpers.
9. **Missing flatten of nested tipb `LogicalAnd`** — Tests that wrap `And(Ge, Le)` as a single filter child as well as flat sibling filters.

## Alternatives

1. **Only use trim as an extra `None` gate while always loading ordinary** — Safer but doubles logic and cannot help when ordinary max is already polluted for complementary checks; rejected in favor of replace-when-eligible (same as DeltaMerge).
2. **Introduce CSE `All` to skip RN filters** — Large semantic change across RN/CSE; out of scope.
3. **Build trim on L0/L1** — Ordinary min-max is L2-only today; expanding levels is a separate project.
4. **Implement eligibility only in TiFlash C++** — CSE owns rough-check; pushing selection to RN would require new tipb annotations and still need CSE correction. Rejected for v1.
5. **Keep a separate `has_value_marks` array in the trim trailer (DeltaMerge-style)** — Works, but wastes one byte per pack and an extra array touch. CSE trim already has a dedicated trailer format, so folding trim has_value into `pack_marks` bit 4 (`TRIM_HAS_VALUE`) — and carrying ordinary has_value as bit 1 in the same unified byte — is preferred. Ordinary **prefix** still expands dual mark arrays for compatibility.
6. **Infer `has_value` from null min/max slots** — Unsafe for NotNull columns whose minmax `ColumnBuffer` may not treat empty packs as null the same way; rejected in favor of an explicit bit.
7. **Skip DateRange normalize; rely on leaf And of one-sided trim** — Incorrectly retains sentinel-inflated packs for BETWEEN; rejected (Phase D is required for range workloads).
8. **Only merge when both lower and upper exist** — Fixes BETWEEN with less code, but diverges from DM and leaves two shapes for one-sided vs two-sided; rejected in favor of full DM parity.
9. **Normalize only inside `prepare_rough_check` without `FilterType::DateRange`** — Smaller enum surface, but tree shape is hard to unit-test and drifts from DM's explicit `DateRange`; rejected.
10. **Always normalize even when trim read is off** — Extra rewrite cost for no benefit; DM gates on the same switch; rejected.

## Established Design Boundaries

- `E = [1900-01-01, 2099-12-01)` half-open; persisted per column trailer.
- Trim trailer appended after ordinary min-max payload inside `compressed_min_max_pack`.
- Ordinary prefix byte-compatible with existing CSE readers; ordinary still uses separate `has_null_marks` + `has_value_marks`.
- Unified `pack_marks` (memory + TRMM): NULL / ORDINARY_HAS_VALUE / TRIMMED_LOW / TRIMMED_HIGH / TRIM_HAS_VALUE; bits 5..7 (`0xE0`) zero; no separate trim `has_value_marks`.
- In-memory temporal columns use `TemporalMinMaxIndex` (ordinary + optional trim view).
- L2-only; temporal types only; read-side `enable_trim_minmax` switch (default off), plumbed from TiFlash `dt_enable_trim_minmax`.
- CSE `FilterOpResult` model unchanged: no `All`; only `None→Some` trim correction.
- Per-leaf eligibility; soft-fallback on logical meta problems.
- Top-level And DateRange normalize when trim reads are enabled; no rewrite under `Or` / `Not`.
- No historical backfill; natural L2 rewrite increases coverage.

## Open Questions

None that block the design shape above. Follow-ups that may be tracked outside this doc:

1. ~~Whether to plumb TiFlash `dt_enable_trim_minmax` into `TableScanCtx::with_enable_trim_minmax` automatically.~~ **Decided:** yes; same setting gates leaf trim reads and DateRange normalize.
2. Whether to later fix `column_props` length parsing as independent cleanup and migrate trailers into first-class column props in a future format version.
3. Whether RN should surface trim-specific counters in `EXPLAIN ANALYZE` beyond existing rough-check pack stats.
