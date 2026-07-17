# Trim Min-Max Index Design for DATETIME, TIMESTAMP, and DATE Types

- Status: Draft
- Date: 2026-07-14

## Summary

This document proposes an optional pack-level `trim_minmax` index for `DATETIME`, `TIMESTAMP`, and `DATE` columns in TiFlash DeltaMerge storage. The index collects statistics only for non-NULL, non-deleted values within a predefined "effective date range." It prevents a small number of extreme sentinel timestamps from polluting the ordinary min-max index. For example, when an application uses `2100-01-01 00:00:00` to mean "unsettled," the presence of this value in a pack of 8,192 rows raises the ordinary min-max `max` to 2100, weakening the Rough Set Filter (RS Filter) for recent, narrow time-range queries.

The first version adopts the following key decisions:

1. The effective date range is the half-open interval `[1900-01-01 00:00:00, 2100-01-01 00:00:00)`.
2. `trim_minmax` stores the min/max of values inside this interval. The ordinary min-max remains unchanged and continues to serve queries that do not satisfy the trim eligibility conditions.
3. The actual bounds, format version, and pack count used by each trim index are persisted directly through `ColumnStat.trim_minmax_index: TrimMinMaxIndexProps`. The `MergedSubFileInfo` associated with the deterministic file name is the sole source of the file location and size. Per-pack low/high flags are merged into the trim index's existing `has_null_marks` byte array, whose on-disk semantics are generalized as `pack_marks`.
4. A reader may select the trim index only according to the actual interval stored in the DMFile. It must not interpret a historical index using the current version's global default interval.
5. A trim index may replace the ordinary min-max for a column only when the reader can prove that out-of-range low and high values each have uniform matching behavior for the predicate. The first version supports equality, IN, and bounded ranges whose match sets are within the effective interval, as well as one-sided ranges whose finite bound is within the effective interval.
6. The reader adjusts the trim rough-check result according to `has_trimmed_low` and `has_trimmed_high`: the presence of a matching trimmed value invalidates `None`, while the presence of a non-matching trimmed value invalidates `All`.
7. The implementation reuses the `MinMaxIndex` payload, serializer, and raw rough check. It adjusts the result through a composition-based trim wrapper instead of making `TrimMinMaxIndex` inherit from `MinMaxIndex`.
8. The first version writes trim indexes only for DMFile V3 / MetaV2. Old DMFiles and unsupported predicates always fall back to the ordinary min-max.

This design does not change SQL semantics, require DDL, or rewrite historical DMFiles. New and old DMFiles can coexist, with each file independently selecting the trim or ordinary min-max within the same query.

## Background

### Current Ordinary Min-Max Generation and Use

`DMFileWriter` currently generates ordinary min-max indexes for handles, integers, and date/time types:

```text
DMFileWriter::write
  -> DMFileWriter::writeColumn
     -> MinMaxIndex::addPack
        -> calculate the column's min/max in the current pack
```

Relevant implementations:

- `dbms/src/Storages/DeltaMerge/File/DMFileWriter.cpp`
- `dbms/src/Storages/DeltaMerge/Index/MinMaxIndex.cpp`

`MinMaxIndex::addPack` ignores values associated with delete marks. Since v6.4.0, it has also excluded NULL from min/max calculation while independently storing `has_null_marks` and `has_value_marks`. The ordinary index for each pack logically contains:

```text
has_null
has_value
min
max
```

On the query side, `FilterParser::parseDAGQuery` converts TiDB DAG predicates into `RSOperator` objects. `DMFilePackFilter` loads min-max indexes for the columns referenced by the predicates and invokes `roughCheck` to produce an `RSResult` for each pack:

```text
None     -> do not read the pack
Some     -> read the pack and execute row-level filtering
All      -> read the pack, but row-level filtering may be skipped
*Null    -> preserve the corresponding NULL semantics
```

Relevant implementations:

- `dbms/src/Storages/DeltaMerge/FilterParser/FilterParser.cpp`
- `dbms/src/Storages/DeltaMerge/Filter/RSOperator.h`
- `dbms/src/Storages/DeltaMerge/File/DMFilePackFilter.cpp`
- `dbms/src/Storages/DeltaMerge/Index/RSResult.h`
- `dbms/src/DataStreams/FilterTransformAction.cpp`

When the RS result for a block is `All`, `FilterTransformAction` directly constructs an all-true filter and does not execute the actual expression. Therefore, every new index must strictly guarantee that `All` means all visible rows in the pack satisfy the predicate. It cannot merely mean that all values retained by the index satisfy the predicate.

### Problem Scenario

Consider a `settle_time DATETIME` column:

- Normal values are within the past 90 days.
- One out of every 10,000 rows uses `2100-01-01 00:00:00` to represent an unsettled record.
- A stable pack contains approximately 8,192 rows by default.
- Sentinel values are uniformly distributed throughout the table.

The probability that a pack contains at least one sentinel value is:

```text
1 - (1 - 1/10000)^8192 ≈ 55.92%
```

Therefore, approximately 55.92% of packs will have:

```text
min = a normal historical timestamp
max = 2100-01-01 00:00:00
```

For the following query:

```sql
settle_time >= L AND settle_time <= U
```

The ordinary min-max can exclude a pack only when either of the following is true:

```text
pack.max < L
pack.min > U
```

The 2100 sentinel breaks the first proof. For a narrow query near the newest end of the time range, many historical packs that are entirely earlier than `L` are incorrectly retained as `Some`. If normal timestamps have good locality within packs, a query for the latest three hours out of the most recent 90 days may increase the theoretical pack read ratio, considering the time predicate alone, from about `1/720 = 0.139%` to about 55.98%.

If normal timestamps are themselves distributed completely at random within each pack, the ordinary min-max already has poor filtering power, so the marginal benefit of the trim index will also be smaller. Rollout validation must therefore cover data with both good and poor temporal locality.

### Key Constraints

1. A specific application sentinel must not be hard-coded into the generic min-max index; doing so could incorrectly prune queries for that value.
2. Readers must support historical DMFiles. Rollout cannot depend on a one-time rewrite of all stable data.
3. During mixed-version operation or rollback, old readers must be able to ignore the new index and continue using the ordinary min-max.
4. `TIMESTAMP` literals are currently converted to UTC by `FilterParser` according to the request time zone. `DATETIME` and `DATE` do not have the same time-zone semantics.
5. The trim index must use the same pack boundaries, NULL rules, and delete-mark rules as the ordinary min-max.
6. `RSResult::All` has the execution semantics of skipping row-level filtering and must not be treated as an ordinary statistical label.

## Terminology

| Term | Definition |
| --- | --- |
| ordinary min-max | The complete min/max of non-NULL, non-deleted values currently stored for each DMFile pack |
| effective date range `E` | The persisted half-open interval `[lower, upper)` used when building a particular trim index |
| trim value | A value within `E` that participates in trim min/max calculation |
| trimmed value | A non-NULL, non-deleted value outside `E` that does not participate in trim min/max calculation |
| `pack_marks` | The per-pack `UInt8` array corresponding to the original `has_null_marks` in a min-max index; bit 0 represents NULL, and a trim index additionally uses bits 1 and 2 |
| `has_trimmed_low` | `pack_marks & 0x02`; indicates that a non-NULL, non-deleted value less than `lower_bound` exists |
| `has_trimmed_high` | `pack_marks & 0x04`; indicates that a non-NULL, non-deleted value greater than or equal to `upper_bound` exists |
| query domain `Q` | The set of all non-NULL temporal values that a column predicate may match |
| trim-eligible | The low and high trimmed values have each been proven to have uniform matching semantics for the predicate, so the flags can safely adjust the trim `RSResult` |

## Goals

1. Restore pack-level filtering for date/time range queries when sparse extreme dates are uniformly distributed.
2. Produce correct `None`, `Some`, and `All` results for trim-eligible predicates without changing query results.
3. Allow different generations of DMFiles to use different effective date ranges and let readers safely select an index per file.
4. Fall back to the ordinary min-max for old DMFiles, unknown index versions, and missing or corrupted trim metadata.
5. Avoid affecting the existing query hot path when disabled. When enabled, add only bounded write, metadata, and cache overhead for relevant date/time columns.
6. Provide sufficient metrics to validate index use, fallback reasons, pack-pruning gains, and write costs.

## Non-Goals

- Changing TiDB SQL semantics or temporal type semantics.
- Requiring users to change DDL or explicitly create an index.
- Supporting `TIME`, durations, strings, or numeric columns in the first version.
- Writing trim indexes for DMFile V1/V2 in the first version.
- Actively backfilling historical DMFiles in the first version. Historical files are naturally rewritten through subsequent merge delta, split, compact, or GC operations.
- Supporting trim predicate analysis for `OR`, `NOT`, `!=`, `NOT IN`, expression columns, or expressions containing casts in the first version.
- Exposing the effective date range as a user-level table or DDL property.

## Correctness Foundation

### Pack-Level Trim Semantics

Let `D` be the set of all values in a pack that participate in the ordinary min-max, and let `E` be the effective date range. The set represented by the trim index is:

```text
D_trim = D ∩ E
```

Let `Q` be the non-NULL match set of the column predicate. When:

```text
Q ⊆ E
```

then:

```text
D ∩ Q = (D ∩ E) ∩ Q = D_trim ∩ Q
```

Therefore:

- When the trim min-max proves that `D_trim ∩ Q` is empty, it may safely return `None`.
- The trim min-max cannot return a final `All` merely because `D_trim ⊆ Q`, because `D - E` may still contain non-matching values.
- For equality, IN, and bounded ranges, low and high trimmed values are both non-matching. Therefore, when a trimmed value exists in either direction, an `All` returned by the trim index must be downgraded to `Some`.
- NULL still participates in `RSResult` composition through the trim index's `has_null` mark and is not a trimmed value.

Directional flags also make it safe to handle one-sided ranges whose bound is within `E`:

| Predicate type | Low trimmed value | High trimmed value |
| --- | --- | --- |
| `col >= T` / `col > T` | Never matches | Always matches |
| `col <= T` / `col < T` | Always matches | Never matches |

Consequently, if the raw trim result is `None` but a trimmed value that must match exists, the result must be downgraded to `Some`. If the raw trim result is `All` but a trimmed value that must not match exists, it must also be downgraded to `Some`. Here, "downgrade" means giving up pack-level certainty and retaining row-level filtering. It does not mean upgrading `None` to `All`.

The following counterexample shows why checking only whether the SQL literal is inside the effective interval is insufficient:

```text
E = [1900, 2100)
pack = {2100-01-01}
predicate = col >= 2020-01-01
```

The literal 2020 is inside `E`, but the query domain is `[2020, +∞)` and includes 2100. Using an empty trim min-max to return `None` would incorrectly omit a matching row. This design uses `has_trimmed_high` to identify a high value that must match and adjusts `None` to `Some`, safely supporting this one-sided comparison.

Another counterexample demonstrates why low/high pack marks are necessary:

```text
E = [1900, 2100)
pack = {2021-01-01, 2100-01-01}
predicate = col BETWEEN 2020-01-01 AND 2022-01-01
```

After trimming, `min=max=2021-01-01`, but the 2100 value in the pack does not match. The trim rough check must return `Some` rather than `All`.

### AND + OR Composition Correctness

Rough Set composition for `And` / `Or` is independent of which physical index each leaf uses. Correctness does **not** require the whole tree to select trim or ordinary uniformly. It requires each leaf's `roughCheck` to be individually sound and conservative; `And` then combines results with `&&` and `Or` with `||`.

#### Per-predicate index selection

Trim indexes may be cached per column in `RSCheckParam.trim_indexes`, but a leaf must not use that trim index unless **its own** `DateQueryDomain` is trim-eligible for the stored `E`. This check is enforced at use time by `getTrimRSIndex(param, attr, query_domain)`:

1. Look up the column's loaded trim index (if any).
2. Call `query_domain.isTrimEligible(stored_lower, stored_upper)`.
3. If ineligible, return no trim index so the leaf falls back to ordinary min-max (when loaded) or `Some`.

Therefore two leaves on the same column may legally use different indexes in one query:

```text
DateRange(t, [L, U]) AND Or(t = A, t = B)
```

| Leaf | Index choice |
| --- | --- |
| `DateRange` | Trim iff `[L, U]` is eligible for stored `E` |
| `Equal(A)` | Trim iff `A ∈ E` |
| `Equal(B)` | Trim iff `B ∈ E` (independent of `A`) |

A dangerous anti-pattern, and the reason eligibility must be per predicate rather than per column, is:

```text
E = [1900, 2100)
pack = {2200-01-01}
predicate = (t = 2020-01-01) OR (t = 2200-01-01)
```

If both equals shared a column-level trim selection without re-checking eligibility, `t = 2200` would consult an empty `D_trim` and could return `None`, causing the whole `Or` to drop a matching pack. With per-predicate eligibility, `t = 2200` must use the ordinary min-max and keep the pack.

#### Normalize interaction with AND / OR

`normalizeTemporalRangesForTrim` only flattens **top-level** `And` nodes and merges exposed temporal `GE` / `GT` / `LE` / `LT` leaves into `DateRange`. It does not rewrite children under `Or` / `Not`.

| Shape | Rewrite behavior | Correctness implication |
| --- | --- | --- |
| `t >= L AND t <= U` | Becomes `DateRange` PreferTrim | Covered by pack-level trim semantics above |
| `t >= L OR t = X` | Unchanged `Or` | No incorrect DateRange merge |
| `t >= L AND t <= U AND (t = A OR t = B)` | Outer bounds → `DateRange`; `Or` kept as a leaf | `DateRange` and each `Equal` still select indexes independently |
| `(t >= L AND t <= U) OR t = X` | Unchanged top-level `Or` | Inner `And` is not rewritten; `Greater`/`Less` leaves continue to use ordinary min-max (they do not request PreferTrim) |

Not rewriting under `Or` is an intentional conservative choice: it may forgo trim benefit, but it must not invent a single range whose match set is not equivalent to the original disjunction.

#### Soundness checklist for mixed trees

For any `And` / `Or` tree that mixes trim-capable and ordinary leaves:

1. Each PreferTrim leaf must re-validate eligibility against stored `E` at `roughCheck` time, not only at load time.
2. Every trim-path result must still apply pack-mark correction (`None`/`All` downgrades for matching / non-matching trimmed values).
3. `All` remains the only RSResult that may skip row-level filtering, so a trim-derived `All` must remain sound after correction.
4. If any leaf cannot prove a safe trim result, it must degrade to ordinary min-max or `Some`; logical composition then stays conservative.

Under these rules, shapes such as `DateRange AND Or(Equal, Equal)` do not introduce an additional correctness hazard beyond the single-predicate trim foundation: composition preserves leaf soundness, and index choice is decided per leaf rather than per column or per logical operator.

## Design

### Overall Architecture

```text
DMFile write path
  -> ordinary MinMaxIndex: all non-NULL, non-deleted values
  -> TrimMinMaxIndex: non-NULL, non-deleted values within E
  -> ColumnStat.trim_minmax_index:
       - TrimMinMaxIndexProps: format version + encoded bounds + pack count
  -> MergedSubFileInfo[<column-stream>.trim.idx]:
       - merged file number + offset + size
  -> trim index pack_marks[pack_count]:
       - bit 0: has_null
       - bit 1: has_trimmed_low
       - bit 2: has_trimmed_high

Query DAG
  -> FilterParser
       - build the existing RSOperator
       - normalize the predicate type and temporal bounds
  -> DMFilePackFilter (per DMFile)
       - predicate bounds are within stored E and low/high semantics are provable: select trim
       - otherwise: select ordinary min-max
  -> roughCheck
       - matching trimmed value + None: Some
       - non-matching trimmed value + All: Some
       - other None/All: unchanged
       - Some: Some
```

### Effective Date Range

The default interval for the first version is:

```text
[1900-01-01 00:00:00, 2100-01-01 00:00:00)
```

A half-open interval is used instead of the closed interval `2099-12-31 23:59:59` because it unambiguously covers fractional seconds in `DATETIME(1..6)`, such as `2099-12-31 23:59:59.999999`.

The bounds are persisted using the column's internal encoding rather than as time strings:

- `DATE` uses the packed value of `DataTypeMyDate`.
- `DATETIME` and `TIMESTAMP` use the packed value of `DataTypeMyDateTime`.
- The field type of both bounds must match the nested type of the indexed column.
- The format contract for `format_version = 1` always interprets the bounds as `[lower, upper)`; the metadata does not separately store boundary semantics.

`TIMESTAMP` query literals continue to use `FilterParser::convertFieldWithTimezone` and are converted into UTC packed values before Rough Set analysis. `DATETIME` and `DATE` are compared as calendar values; "UTC+0" is not part of their type semantics.

### TrimMinMaxIndex Data Model

The trim min-max reuses the core representation of the ordinary `MinMaxIndex`:

```text
pack_marks[pack_count]       // same physical position as the existing has_null_marks
has_value_marks[pack_count]
minmaxes[pack_count * 2]
```

The V1 bit layout of `pack_marks` is:

| Bit | Mask | Ordinary min-max | Trim min-max |
| --- | --- | --- | --- |
| 0 | `0x01` | `has_null` | `has_null` |
| 1 | `0x02` | Must be 0 | `has_trimmed_low` |
| 2 | `0x04` | Must be 0 | `has_trimmed_high` |
| 3..7 | `0xf8` | Must be 0 | Must be 0; reserved |

The remaining fields have the following semantics:

- `has_value_marks` indicates whether the pack contains at least one non-NULL, non-deleted value within `E`.
- `minmaxes` is calculated only from values within `E`.
- NULL, low/high flags, and the trim min/max use the same pack order and all reside in the same trim index payload.

The byte layout and existing content of the ordinary `.idx` remain unchanged: historical mark values are already either `0x00` or `0x01`. After generalizing the internal concept from `has_null_marks` to `pack_marks`, every NULL check in the code must use `(pack_marks[i] & 0x01) != 0` instead of treating the entire byte as a Boolean; otherwise, a low/high bit would be misinterpreted as NULL. Access through `hasNull()`, `hasTrimmedLow()`, and `hasTrimmedHigh()` accessors is recommended.

`TrimMinMaxIndex` is not a subclass of `MinMaxIndex`. The current `MinMaxIndex` state is private, `read()` always constructs the base class, `checkCmp` is a non-virtual template method, and the query path calls through `MinMaxIndexPtr`. Adding virtual interfaces and a specialized factory for inheritance would not simplify the disk format. The first version uses composition:

```cpp
struct TrimMinMaxIndex
{
    MinMaxIndexPtr minmax;

    RSResults roughCheck(
        TrimPredicateClass predicate_class,
        size_t start_pack,
        size_t pack_count) const;
};
```

`MinMaxIndex` performs generic serialization and the raw min-max check. The trim wrapper or `DateRange` operator reads the pack-mark accessors and adjusts `None` / `All`. The wrapper stores no additional per-pack arrays.

The trim index uses a separate subfile name, logically:

```text
<column-stream>.trim.idx
```

In DMFile V3, this small file is written into a merged file just like the ordinary min-max and marks. `MergedSubFileInfo` stores its physical file number, offset, and size.

Trim data is not appended to the existing `.idx`. `MinMaxIndex::read` currently requires the number of bytes actually consumed to match the ordinary `index_bytes` exactly. Directly extending the existing file would break old readers.

### `ColumnStat.trim_minmax_index` Metadata

The first version neither adds a separate MetaV2 block nor appends trim to the existing `ColumnStat.indexes = 104`. Instead, `dmfile.proto` adds an independent optional field to `ColumnStat` whose type is directly `TrimMinMaxIndexProps`:

```protobuf
message ColumnStat {
    // existing fields...
    repeated DMFileIndexInfo indexes = 104;

    // Internal DMFile trim min-max; at most one per column.
    optional TrimMinMaxIndexProps trim_minmax_index = 105;
}

message TrimMinMaxIndexProps {
    optional uint32 format_version = 1;

    // Internal encoding matching the nested type of the owning ColumnStat.
    optional bytes lower_bound = 2;
    optional bytes upper_bound = 3;

    optional uint64 pack_count = 4;
}
```

The actual field number will be assigned during implementation according to protobuf compatibility rules; the structure above defines the data contract. `TrimMinMaxIndexProps` contains information specific to an internal DMFile pack index. It does not reuse `DMFileIndexInfo`, `IndexFilePropsV2`, `IndexFileKind`, or the DDL local-index lifecycle.

Field sources and omission rules:

- The column ID comes from the enclosing `ColumnStat.col_id` and is not stored again.
- The file name is deterministically derived from the column ID as `<column-stream>.trim.idx`.
- The physical merged-file number, offset, and size are stored only in the `MergedSubFileInfo` corresponding to that file name. `TrimMinMaxIndexProps` does not duplicate the file size.
- Trim is an internal index. It does not correspond to a TiDB DDL index, has no kind or index ID, and does not enter local-index APIs that query by index ID.
- Per-pack flags exist only in the trim index payload's `pack_marks`; protobuf does not store a per-pack array.

Metadata constraints:

1. `format_version = 1` always uses the half-open interval `[lower_bound, upper_bound)`. Any future change to boundary semantics must increment the version. A reader falls back to the ordinary min-max when it encounters an unknown version.
2. The bounds must be decodable as the owning column's nested type and must satisfy `lower_bound < upper_bound`.
3. `pack_count` must equal the number of packs in the DMFile.
4. The `.trim.idx` file name derived from the owning column must be present in `MergedSubFileInfo`, with a valid number, offset, and size. This size is the sole source of the file size for loading and checksum-frame calculation.
5. The counts of decoded `pack_marks`, `has_value_marks`, and min/max cells must agree and equal `pack_count`.
6. Bits 3 through 7 of the trim index's `pack_marks` must be zero.
7. The `ColumnStat` metadata, `MergedSubFileInfo`, and index payload must be generated and published atomically as part of the same immutable DMFile generation. A reader must not reuse or combine them across DMFiles.
8. The trim index must not be used if the metadata, `MergedSubFileInfo`, or subfile is missing; the version is unknown; or any structural validation fails.

The ColumnStat protobuf is protected by the DMFileMetaV2 checksum, while the trim index subfile uses the existing DMFile checksum mechanism. These checks are combined with structural validation of the pack count, pack marks, and subfile location and size to detect physical corruption.

The compatibility-critical property is that field 105 does not belong to `indexes = 104`, which old versions iterate over and validate strictly. Old readers treat all of field 105 as an unknown protobuf field. It does not trigger `ColumnStat::integrityCheckIndexInfoV2` and does not require recognition of a new `IndexFileKind`. Old readers continue to read the ordinary min-max.

This compatibility guarantees safe reading only. After converting protobuf into an old C++ `ColumnStat`, an old version will not preserve field 105. If an old node subsequently rewrites ColumnStat metadata, for example while adding a local index to a DMFile, it may discard the trim metadata. A new reader must then treat `.trim.idx` as unavailable and fall back to the ordinary min-max. The remaining subfile is only a space issue and is reclaimed when the DMFile is later rewritten; it must not affect query correctness.

No field is added directly to `PackStat` or `PackProperty`. MetaV2 currently serializes these structures using their raw POD layouts, so changing `sizeof` would break the old format.

### Write Path

For each supported temporal column, `DMFileWriter::Stream` adds an optional trim `MinMaxIndex` builder. The ordinary and trim min-max values are calculated during the same pack traversal to avoid scanning the column twice. This reuses the builder and serializer and does not extend `MinMaxIndex` through inheritance.

Pseudocode:

```cpp
UInt8 trim_pack_mark = 0;

for (row : pack)
{
    if (isDeleted(row))
        continue;

    if (isNull(row))
    {
        ordinary.has_null = true;
        trim_pack_mark |= 0x01; // has_null
        continue;
    }

    ordinary.updateMinMax(row.value);

    if (lower <= row.value && row.value < upper)
        trim.updateMinMax(row.value);
    else if (row.value < lower)
        trim_pack_mark |= 0x02; // has_trimmed_low
    else
        trim_pack_mark |= 0x04; // has_trimmed_high
}
```

After each pack is written:

1. The ordinary min-max appends one cell through the existing path.
2. The trim min-max appends one cell. If there is no in-range value, `has_value=false`.
3. `trim_pack_mark` is appended to the trim index's `pack_marks`.
4. During finalization, the writer writes the trim subfile, registers its `MergedSubFileInfo`, and stores `TrimMinMaxIndexProps` in the owning `ColumnStat.trim_minmax_index`.

The implementation adds an internal `appendPack(pack_mark, has_value, min, max)` or equivalent builder API to `MinMaxIndex` for use by both ordinary and trim writers. The ordinary path permits only `pack_mark & ~0x01 == 0`; the trim path permits `0x01 | 0x02 | 0x04`. Deserialization continues to reuse the generic payload reader, after which the trim loader validates the mark mask and pack count.

No trim index is generated for:

- Internal columns such as handle, version, and delete mark.
- `TIME` and duration.
- Nested types other than `MyDate` / `MyDateTime`.
- Empty DMFiles.
- Any column when `dt_enable_trim_minmax` is disabled.

During finalization, the writer may check whether a column has any trimmed value anywhere in the DMFile. If all `pack_marks & 0x06` values are zero, the ordinary and trim min-max indexes are equivalent for non-NULL values. The trim subfile and metadata may then be omitted, avoiding storage overhead for files without abnormal values. The bit-0 NULL mark does not affect this decision.

### Query-Domain Analysis

`FilterParser` currently converts each comparison expression independently into operators such as `GreaterEqual` and `LessEqual`. A single literal is insufficient to determine whether the complete query domain belongs to the effective interval, so a temporal column query-domain normalization step is required.

The first version supports the following trim-eligible forms:

```sql
time_col = T
time_col IN (T1, T2, ...)
time_col >= L AND time_col <= U
time_col > L AND time_col < U
time_col >= L
time_col > L
time_col <= U
time_col < U
```

The rules are:

- The query domain for equality is the singleton `{T}`.
- All non-NULL values in an IN predicate must be within the stored `E`.
- After type and time-zone conversion, a bounded range's lower and upper bounds must form `Q ⊆ E`.
- The bound of a lower-bounded range must be within the stored `E`. A low trimmed value then never matches, while a high trimmed value always matches.
- The bound of an upper-bounded range must be within the stored `E`. A low trimmed value then always matches, while a high trimmed value never matches.
- The lower and upper bounds may come from the same `LogicalAnd`, or from the top-level AND ultimately formed by `DAGQueryInfo::filters` and `pushed_down_filters`.
- When multiple lower or upper bounds exist for the same column, select the semantically strongest lower and upper bounds.
- In the first version, no trim query domain is generated for a branch involving OR, NOT, NotEqual, NotIn, IsNull, a function, or a cast.

A new `DateRange` RSOperator should represent an already-normalized date range, including bounded and one-sided ranges. This operator affects only the rough check. The actual row-level filter continues to be built from the original DAG expression; the SQL execution expression is not changed.

To allow `DMFilePackFilter` to select a normal or trim index per DMFile, extend the index request interface so that an operator can declare:

```cpp
struct RSIndexRequest
{
    ColId col_id;
    RSIndexKind preferred_kind; // Normal or PreferTrim
    std::optional<DateQueryDomain> query_domain;
};
```

`RSCheckParam` stores normal and trim indexes separately, preventing an overwrite when the same column participates in both a trim-eligible range and another ordinary predicate:

```cpp
struct RSCheckParam
{
    ColumnIndexes normal_indexes;
    TrimColumnIndexes trim_indexes; // values contain composition-based TrimMinMaxIndex wrappers
    TrimMinMaxInfos trim_infos; // parsed from ColumnStat.trim_minmax_index
};
```

If the query domain of a temporal operator satisfies the trim conditions for a DMFile, only the trim index is loaded by preference. If another operator on the same column also needs the ordinary min-max, both types may coexist. Index caches use distinct keys containing at least a stable DMFile identity, the column ID, and the index kind. Because a DMFile is an immutable generation, the bounds and the index payload containing pack marks cannot change independently under the same identity.

### Per-DMFile Index Selection

For every temporal column request and every DMFile, independently execute:

```cpp
if (!trim_read_enabled)
    choose NORMAL;
else if (!trim_meta_exists(col_id))
    choose NORMAL;
else if (!readerSupports(trim_meta.format_version))
    choose NORMAL;
else if (!metaAndIndexAreConsistent(trim_meta))
    choose NORMAL;
else if (!query_domain.isTrimEligible(trim_meta.range))
    choose NORMAL;
else
    choose TRIM;
```

`isTrimEligible` is not equivalent to a simple `Q ⊆ E` check. Equality, IN, and bounded ranges still require their complete match sets to be inside `E`. A one-sided range instead requires its finite bound to be within `E` and passes the predicate classification to the rough check so that low and high trimmed values can be identified as "always matching" or "never matching."

Selection must use `trim_meta.range` rather than the current process's default range. This safely supports:

```text
DMFile A: E=[1900, 2100), use trim
DMFile B: no trim, use normal
DMFile C: future version E=[1800, 2200), decide according to C's metadata
```

If the reader does not support the metadata version or validation fails, it falls back to the ordinary min-max rather than returning a speculative result other than `Some`. If the underlying index payload checksum is definitively corrupted, the existing DMFile data-corruption policy still applies; physical corruption is not silently hidden.

### Trim Rough Check and `pack_marks`

For each pack, the reader decodes:

```cpp
const UInt8 pack_mark = trim_index.minmax->packMark(pack_id);
const bool has_null = (pack_mark & 0x01) != 0;
const bool has_trimmed_low = (pack_mark & 0x02) != 0;
const bool has_trimmed_high = (pack_mark & 0x04) != 0;
```

Based on the predicate class, it determines whether a trimmed value that must match or must not match exists:

| Predicate type | `trimmed_match_exists` | `trimmed_nonmatch_exists` |
| --- | --- | --- |
| Equality / IN / bounded range with `Q ⊆ E` | false | `has_trimmed_low || has_trimmed_high` |
| Lower-bounded range with bound in `E` | `has_trimmed_high` | `has_trimmed_low` |
| Upper-bounded range with bound in `E` | `has_trimmed_low` | `has_trimmed_high` |

The trim min-max first calculates a result using the existing RoughCheck rules and then applies these conservative adjustments:

| Raw trim result | Condition | Final result |
| --- | --- | --- |
| `None` | `trimmed_match_exists=false` | `None` |
| `None` | `trimmed_match_exists=true` | `Some` |
| `NoneNull` | `trimmed_match_exists=false` | `NoneNull` |
| `NoneNull` | `trimmed_match_exists=true` | `SomeNull` |
| `Some` / `SomeNull` | Any | Unchanged |
| `All` | `trimmed_nonmatch_exists=false` | `All` |
| `All` | `trimmed_nonmatch_exists=true` | `Some` |
| `AllNull` | `trimmed_nonmatch_exists=false` | `AllNull` |
| `AllNull` | `trimmed_nonmatch_exists=true` | `SomeNull` |

These rules only downgrade a certain result to `Some`. The flags never upgrade `None` to `All` or `Some` to a certain result. This safely handles one-sided queries even when a trim pack has no in-range value. For example, for a pack containing only 2100 and `col >= 2020`, the raw trim result may be `None`, but `has_trimmed_high` adjusts it to `Some`. A bounded-range query can still leave this pack as `None` and skip it.

### NULL, Deletion, and MVCC

The trim index follows the current ordinary min-max rules:

- NULL does not participate in min/max.
- A non-deleted NULL sets the trim index's `has_null`.
- NULL does not set `has_trimmed_low` or `has_trimmed_high`.
- Values associated with delete marks participate in neither normal nor trim and do not set the low/high bits in `pack_marks`.
- `has_value=false` is set when a pack contains no non-deleted in-range value.

This keeps the trim and ordinary min-max indexes consistent in their three-valued RS semantics. If the ordinary min-max definition of the set of MVCC-visible values changes in the future, trim generation must change with it; the two indexes must not represent different row sets.

### Configuration and Switches

The first version provides one internal setting:

```text
dt_enable_trim_minmax
```

- When enabled, new DMFiles may generate trim indexes, and readers may select trim (including temporal-range normalize for PreferTrim).
- When disabled, writers skip trim generation, and readers ignore existing trim metadata/subfiles and fall back to the ordinary min-max.
- Default is disabled; enable gradually through canaries. Note that a single switch means write and read are rolled out together.
- The effective interval is not dynamically configurable in the first version, avoiding configuration drift within a process. It is still persisted in metadata to preserve correctness during future format evolution.

### Observability

Add query- or instance-aggregated counters and timings:

```text
trim_minmax_index_load_count
trim_minmax_index_load_bytes
trim_minmax_index_load_time
trim_minmax_selected_packs
trim_minmax_none_packs
trim_minmax_some_packs
trim_minmax_all_packs
trim_minmax_none_downgraded_packs
trim_minmax_all_downgraded_packs
trim_minmax_fallback_count{reason}
trim_minmax_metadata_lost_count
trim_minmax_orphan_subfile_count
trim_minmax_write_bytes
trim_minmax_write_time
```

`fallback reason` must distinguish at least:

```text
disabled
no_meta
unsupported_version
predicate_boundary_outside_range
unsupported_expression
metadata_mismatch
index_missing
```

Debug logs record the DMFile, column ID, stored range, query domain, selected index type, and pack-filtering ratio. In the long term, trim pack statistics should be incorporated into `ScanContext` so they can be displayed by `EXPLAIN ANALYZE`. In the first phase, ProfileEvents, instance metrics, and debug logs are sufficient for validation.

## Compatibility and Invariants

### Query-Correctness Invariants

1. A trim index must not make any originally matching row disappear.
2. A trim index must not allow a non-matching trimmed value into the result through `All`.
3. Equality, IN, and bounded ranges may select trim only when `Q ⊆ stored E`. A one-sided range may select trim only when its finite bound is within stored `E` and the low/high matching semantics are known.
4. A reader must not use trim if it cannot verify consistency between the metadata and index payload.
5. The row-level filter is always retained and may be skipped only when the final RSResult is a strictly correct `All`.
6. Boundary semantics for `format_version = 1` are always `[lower_bound, upper_bound)` and must not be reinterpreted by runtime configuration.
7. NULL semantics read only `pack_marks & 0x01`. Low/high bits must not affect NULL checks for the ordinary min-max.

### Disk-Format Compatibility

- The ordinary `.idx` format is unchanged, and old readers continue to read the ordinary min-max.
- Pack marks in an ordinary `.idx` remain restricted to `0x00` / `0x01`. Extended trim bits appear only in the separate `.trim.idx`.
- Trim uses a separate subfile, so an old reader does not interpret extra bytes as part of the ordinary index.
- Trim metadata uses independent optional field 105 in `ColumnStat` and is not added to `indexes = 104`, which old readers validate strictly.
- Old readers ignore field 105. New readers fall back to normal when reading old DMFiles that lack the field.
- `format_version` versions both the serialized structure and boundary semantics. Unsupported versions fall back to normal.
- The first version does not modify the raw `PackStat` / `PackProperty` layout.
- The first version writes only V3 / MetaV2, avoiding simultaneous extensions to the V1/V2 metadata formats.

### Rolling Upgrade and Downgrade

Recommended sequence:

1. First deploy new binaries capable of parsing `ColumnStat.trim_minmax_index`, with `dt_enable_trim_minmax` disabled (default).
2. Enable `dt_enable_trim_minmax` on a small canary so those nodes both generate trim on new DMFiles and select trim when eligible.
3. Verify that old readers ignore field 105 and read the ordinary min-max. Also verify that, if an old node rewrites metadata and loses trim information, a new reader safely falls back.
4. Expand the rollout.

For downgrade, disable `dt_enable_trim_minmax` first. Existing trim subfiles and field 105 do not affect the ordinary min-max. Compatibility tests must verify both "new write, old read" and "old-version metadata rewrite." If the target old version cannot safely ignore field 105 or an additional merged subfile, the switch must remain disabled during mixed-version operation. Losing trim metadata during an old-version rewrite is an allowed safe degradation, but it must be recorded in metrics and the corresponding loss of trim benefit must be accepted for that DMFile.

## Performance and Resource Overhead

### Index Space

For `MyDateTime`, the trim min-max for each pack contains approximately:

```text
min + max        16 bytes
pack_marks        1 byte
has_value_marks   1 byte
```

With the current byte-array representation of `MinMaxIndex`, the uncompressed size is approximately 18 bytes per pack per column. With 8,192 rows per pack:

```text
18 / 8192 ≈ 0.0022 bytes/row/column
```

The low/high flags reuse the trim min-max's existing per-pack `has_null_marks` byte. Therefore, they add no bytes relative to a trim min-max payload and do not make MetaV2 grow with the number of packs. For a DMFile with about one million rows, 123 packs, and five temporal columns, this saves approximately `123 × 5 = 615` bytes of MetaV2 content compared with storing the flags separately in metadata.

### Write CPU

If min/max is scanned twice independently, index-building CPU for temporal columns may nearly double. The design requires normal and trim to be updated during one traversal. The common path adds only two bound comparisons and one conditional branch.

### Reads and Cache

- If a query is trim-eligible and the column has no other normal-index request, load only trim and not the ordinary min-max.
- A fallback query loads only the ordinary index.
- If the same column has both a trim-eligible predicate and another ordinary predicate, both indexes may be loaded.
- Trim and ordinary use distinct cache keys. Pack marks are already included in the first byte array accounted for by `MinMaxIndex::byteSize()` and do not require separate cache weight.
- A DMFile with no trimmed value does not persist trim, avoiding overhead with no benefit.

## Phased Implementation and Rollout

### Phase A: Format and Compatibility

- Add `TrimMinMaxIndexProps` and `ColumnStat.trim_minmax_index = 105` and implement trim-specific parsing and validation. Do not modify `DMFileIndexInfo`, `IndexFilePropsV2`, or `IndexFileKind`.
- Add trim subfile naming, merged-file location, and cache keys.
- Consolidate internal `has_null_marks` reads behind pack-mark accessors and verify that ordinary min-max interprets only bit 0.
- Ensure new readers can read old files and fall back to normal for every trim anomaly.
- Keep read/write switches disabled by default.
- Complete tests for new-write/old-read, old-write/new-read, old-version metadata rewrite, CN/disaggregated file paths, and checksums.

### Phase B: Index Writing

- Generate normal, trim, and trim `pack_marks` in one traversal in `DMFileWriter`.
- Generate them only for V3 user columns of `MyDate` / `MyDateTime` types.
- Canary writes and observe write throughput, CPU, DMFile size, and metadata size.

### Phase C: Query Domain and Read Path

- Add top-level AND temporal-range normalization and the `DateRange` operator.
- Use a composition-based trim wrapper to invoke the generic `MinMaxIndex` without introducing a virtual base interface or derived-class deserialization.
- Support trim eligibility for equality, IN, bounded ranges, and one-sided ranges.
- Select indexes according to each DMFile's stored range.
- Use low/high flags to conservatively implement `None -> Some`, `NoneNull -> SomeNull`, `All -> Some`, and `AllNull -> SomeNull` adjustments.
- Canary reads and compare query results, scanned rows, and pack-filtering ratios.

### Phase D: Default Enablement and Natural Migration

- Gradually enable reads and writes by default after compatibility and performance thresholds are met.
- Continue using normal for old DMFiles without a full backfill.
- Naturally increase trim coverage through existing merge delta, split, compact, and GC operations.
- Retain the read/write kill switch for at least one full release cycle.

## Validation Strategy

### Unit Tests

#### TrimMinMaxIndex

Cover at least these packs:

```text
all values within E
all values below E
all values above E
both low and high outliers
normal value + 2100 sentinel
NULL only
NULL + normal value + outlier
delete mark + normal value + outlier
no valid values
```

Verify min/max, `has_value`, pack-mark accessors, and serialization round trips. Cover at least `0x00`, `0x01` (NULL), `0x02` (low), `0x04` (high), `0x06` (low + high), and `0x07` (NULL + low + high). A trim V1 reader must reject an index payload with nonzero bits 3 through 7. Historical ordinary `.idx` values `0x00` / `0x01` must remain compatible.

#### RSResult

Verify in particular:

```text
pack={2021, 2100}, query=[2020, 2022] -> Some; must not be All
pack={2100}, query=[2020, 2022]       -> None
pack={2021}, query=[2020, 2022]       -> All
pack={NULL, 2021}, query=[2020, 2022] -> AllNull or an equivalent result requiring row-level filtering
pack={2100}, query>=2020               -> Some; must not be None
pack={1800}, query>=2020               -> None
pack={1800}, query<=2020               -> Some; must not be None
pack={2100}, query<=2020               -> None
pack={1800, 2100}, query>=2020         -> Some
```

#### Query-Domain Analysis

Trim may be used for:

```sql
col = '2020-01-01'
col IN ('2020-01-01', '2021-01-01')
col >= '2020-01-01' AND col <= '2020-01-02'
col >= '2020-01-01'
col < '2020-01-01'
```

Must fall back:

```sql
col >= '2200-01-01'
col <= '1800-01-01'
col != '2020-01-01'
NOT (col BETWEEN ...)
col BETWEEN ... OR status = 1
col BETWEEN ... OR col IS NULL
CAST(col AS ...) = ...
```

### Temporal-Type Tests

- `DATE`, `DATETIME(0)`, `DATETIME(3)`, and `DATETIME(6)`.
- `TIMESTAMP` in UTC, fixed-offset, and named time zones, including DST boundaries.
- The lower and upper bounds themselves.
- `2099-12-31 23:59:59.999999`.
- `2100-01-01 00:00:00`.
- Zero dates, invalid-date compatibility values, and other out-of-range packed values.
- Nullable and NotNull.

### DMFile Format and Compatibility Tests

- Old DMFile -> new reader.
- New DMFile -> old reader within the supported compatibility range.
- Missing `ColumnStat.trim_minmax_index`, unknown version, and incorrect `pack_count`.
- Field 105 must not appear in `indexes = 104`, and an old reader must not enter `integrityCheckIndexInfoV2` for it.
- Missing `MergedSubFileInfo` for the deterministic `.trim.idx` file name, or invalid number/offset/size.
- An old reader rewrites ColumnStat and drops field 105; a new reader must fall back to normal when reopening it.
- A residual `.trim.idx` must not be used after trim metadata is lost, and its space should be reclaimed after the DMFile is naturally rewritten.
- The trim index's pack-mark count does not equal `pack_count`, or bits 3 through 7 are nonzero.
- Bounds cannot be decoded, or `lower_bound >= upper_bound`.
- Metadata or index-subfile checksum mismatch.
- Local disk and disaggregated storage.
- Reopening merged subfiles, clone, restore, GC, and segment replacement.
- Online switching and fallback of the read/write settings.

### Query-Result Tests

Run the same dataset with:

```text
trim reads disabled
trim reads enabled
forced fallback to normal
```

Compare complete result sets rather than only row counts. Cover SELECT, aggregation, TopN, LIMIT, concurrent reads, partitioned tables, and multiple DMFiles with mixed versions.

### Performance Tests

Construct data with:

- Pack size 8,192.
- Normal timestamps covering 90 days with temporal locality.
- One out of every 10,000 rows using the 2100 sentinel, uniformly distributed.
- Queries covering the latest consecutive three hours.

Compare:

```text
ordinary min-max
ordinary + trim disabled
trim enabled
no-outlier baseline
```

Collect at least:

- Counts of RS none/some/all packs.
- DMFile scanned/skipped rows and bytes.
- Index load bytes/time/cache hits.
- Query p50/p95/p99 latency.
- Merge delta / compact write throughput and CPU.
- DMFile data, index, and metadata sizes.

Do not use the theoretical 403x pack reduction as a hard acceptance threshold because it depends on temporal locality. The hard requirements are identical query results, no significant regression for workloads without outliers, and scanned rows for the target dataset that are clearly close to the no-outlier baseline.

## Risks and Mitigations

### Interpreting an Old Index Using the Current Default Interval

Risk: The effective interval changes during product evolution, and a new reader produces a false negative from an old trim index.

Mitigation: Persist the actual bounds for every column in every DMFile. Eligibility uses only the stored range, and the V1 format always uses a half-open interval. Metadata and the index payload containing pack marks are atomically published with the same immutable DMFile generation. Existing checksums and structural validation detect corruption or incorrect combinations.

### Failure to Adjust `None` or `All` Using Directional Flags

Risk: After trim ignores outliers, incorrectly retaining `None` can omit matching trimmed rows. Incorrectly retaining `All` can allow non-matching trimmed rows to bypass row-level filtering.

Mitigation: Persist per-column, per-pack low/high flags. Downgrade `All` to `Some` when a trimmed value that must not match exists, and downgrade `None` to `Some` when a trimmed value that must match exists. Add dedicated result-consistency tests.

### Incomplete Query-Domain Analysis

Risk: Incorrect predicate classification can reverse the matching direction of low/high trimmed values or incorrectly apply trim to an OR query.

Mitigation: In the first version, allowlist equality, IN, top-level AND bounded ranges, and one-sided ranges whose bounds are within `E`. Explicitly test all four comparison directions and packs containing both low and high values. All other expressions fall back.

### Mixed-Version Read Failure

Risk: An old reader cannot ignore field 105 or the additional merged subfile and therefore cannot open the DMFile.

Mitigation: Trim is not added to `indexes = 104`; an old reader only needs to ignore the new optional field. The ordinary `.idx` and raw PackStat remain unchanged. Writes remain disabled until new-write/old-read and mixed-version reopen tests pass.

### Trim Metadata Lost During an Old-Version Rewrite

Risk: An old node converts protobuf into an old C++ `ColumnStat` without field 105 and later rewrites the metadata during operations such as local-index building, losing trim metadata and leaving an unreachable `.trim.idx`.

Mitigation: A new reader must fall back to the ordinary min-max when metadata is absent. Record trim-metadata-lost and orphan metrics. Cover this path with clone, restore, local-index bump, and GC tests. Orphaned space is reclaimed when the DMFile is later rewritten by merge/compact/GC. This condition may reduce performance only and must not affect results.

### Independent Field Diverges from the Generic Index Registry

Risk: Because `trim_minmax_index = 105` is not in `indexes = 104`, existing tools, diagnostics, and generic lifecycle APIs that iterate local indexes will not automatically see trim.

Mitigation: Provide trim-specific ColumnStat accessors, validation, and diagnostic output. Explicitly exclude trim from DDL index IDs, the asynchronous local-index scheduler, and `getLocalIndex(index_id)`. Consider migration to the unified registry only after the minimum supported reader version can ignore unknown kinds.

### Write CPU or Cache Overhead

Risk: The additional index may offset the query benefit, especially for workloads without outliers.

Mitigation: Generate both indexes in one traversal, omit trim when an entire file has no trimmed value, lazily load normal and trim, and provide independent read/write kill switches.

### Temporal Bound or Time-Zone Inconsistency

Risk: The writer and reader disagree on the packed representation of bounds or on `TIMESTAMP` literal conversion.

Mitigation: Store typed packed bounds in metadata. The reader determines the query domain after existing time-zone conversion. Cover FSP, UTC, offset, and DST in tests.

### Trim Index and MetaV2 Space Growth

Risk: The additional trim index increases DMFile size, and many temporal columns still add fixed-size column metadata to MetaV2.

Mitigation: Low/high flags reuse the trim index's existing pack-mark byte, and MetaV2 stores no per-pack data. Do not write trim indexes or metadata for columns without outliers. Add metrics for index/metadata size and parsing time.

## Alternatives

### Use NULL to Represent Unsettled Values

The current ordinary min-max already excludes NULL, so this is the simplest solution when the application can change its data model. However, existing schemas, application compatibility, and other sentinel-value scenarios may prevent a uniform migration, so this cannot replace a general storage-layer optimization.

### Reduce the Pack Size

Reducing the number of rows per pack lowers the probability of outlier contamination, but it increases the number of packs, index size, marks, and read/write scheduling overhead. It also does not eliminate contamination by extreme values.

### Hard-Code the Ordinary Min-Max to Ignore 2100

This would produce incorrect results for queries that target or cover 2100 and cannot generalize to other sentinel values. It is rejected.

### Use Trim Only as an Additional `None` Gate

This is the easiest approach to make correct: trim only excludes packs, while all other results come from the ordinary min-max. However, it requires loading both ordinary and trim indexes and cannot restore a correct `All`. By persisting low/high flags in the trim index's pack marks, this design allows trim to safely replace the ordinary min-max for eligible queries and therefore chooses the complete RSResult approach.

### Store Two Independent Bitmaps

Separate `has_trimmed_low_bitmap` and `has_trimmed_high_bitmap` values require managing two lengths, tail bits, address calculations, and storage locations. The first version reuses the trim min-max's existing `has_null_marks` byte: bit 0 preserves NULL semantics, while bits 1 and 2 represent low and high. This adds no per-pack byte and stores no bitmap in MetaV2.

### Make `TrimMinMaxIndex` Inherit from `MinMaxIndex`

The current `MinMaxIndex` state is private, `read()` always constructs the base class, `checkCmp` is a non-virtual template method, and the query path invokes it statically through `MinMaxIndexPtr`. Adding a virtual destructor, virtual interfaces, protected state, and a derived-class factory solely for inheritance would expand changes to the ordinary min-max hot path and cache without changing the disk layout. The first version therefore rejects inheritance and uses composition: a `MinMaxIndex` payload plus a lightweight trim wrapper.

### Append Trim Directly to `ColumnStat.indexes`

This would let trim share a unified registry with vector, inverted, and full-text indexes, but it requires a new index kind. Current old readers call `integrityCheckIndexInfoV2` on every element of `indexes = 104`, and an unknown kind causes DMFile restore to fail. The first version cannot use this option unless a reader generation that ignores and preserves unknown kinds is deployed first and rollback to older versions is no longer required. Option B uses a self-contained `TrimMinMaxIndexProps` directly in field 105, bypassing strict traversal by old readers and avoiding dependencies on DDL local-index metadata.

## Established Design Boundaries

- The effective interval is the half-open interval `[1900-01-01, 2100-01-01)`.
- Actual bounds are persisted per column and per DMFile. Readers do not use the current default configuration to interpret old indexes.
- The trim index defines the original `has_null_marks` byte array as `pack_marks`: bit 0 is NULL, bit 1 is low, bit 2 is high, and bits 3 through 7 must be zero in V1.
- Trim metadata uses `ColumnStat.trim_minmax_index = 105`, whose type is directly the self-contained `TrimMinMaxIndexProps`. It is not added to `indexes = 104` and does not modify `DMFileIndexInfo`, `IndexFilePropsV2`, or `IndexFileKind`.
- The `MergedSubFileInfo` associated with the deterministic `.trim.idx` file name is the sole source of its location and size; the props do not duplicate the file size.
- Field 105 stores no per-pack flags. Low/high flags reside in the trim index payload together with min/max.
- The trim min-max uses a separate subfile and does not extend the ordinary `.idx`.
- `TrimMinMaxIndex` uses composition rather than inheritance. Generic `MinMaxIndex` produces the raw result, and the trim wrapper/operator applies directional adjustments.
- The first version writes only DMFile V3 / MetaV2.
- The first version uses trim for allowlisted equality, IN, bounded ranges, and one-sided ranges whose bounds are within `E`. Other predicates fall back.
- Low/high flags determine whether trimmed values must match or must not match and conservatively adjust `None` and `All`.
- Old files are not backfilled; coverage grows through natural rewrites.
