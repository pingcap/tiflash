# TiFlash NullEQ Join Key (`<=>` / `tidbNullEQ`) Design

## Background

The default equality semantics of TiFlash hash join are currently:

- `NULL` does not participate in equality matching.
- Build-side rows with a `NULL` key are not inserted into the hash map.
- Probe-side rows with a `NULL` key are treated directly as not matched.

This differs from the semantics required by null-safe equality (`<=>` / `tidbNullEQ`):

- `NULL <=> NULL` is `true`.
- `NULL <=> non-NULL` is `false`.
- `non-NULL <=> non-NULL` has the same semantics as ordinary `=`.

This document discusses how to add **join-key-level NullEQ semantics** to hash join,
assuming that TiFlash already supports `FULL OUTER JOIN`.

## Goals

1. Make TiFlash hash join produce correct results when a **join key** uses NullEQ semantics.
2. Support **mixed semantics at key granularity**:
   - Some keys in the same join may use `=`.
   - Other keys may use `<=>`.
3. Preserve the existing behavior when no NullEQ flags are sent.
4. Remain compatible with the existing `FULL OUTER JOIN` semantics and avoid incorrect
   results related to `NULL <=> NULL` matching.

## Non-goals

1. `<=>` in `other_conditions` is outside the new join-key NullEQ semantics covered by
   this document. If the planner sends such an expression, it is handled as an ordinary
   other condition.
2. The MVP does not pursue optimal performance and may force the `serialized` method for
   correctness.
3. The MVP does not expand the semantic coverage of NullAware joins (`NOT IN` family).
   Fail-fast is recommended at the MVP stage.
4. The combination of cartesian full join and NullEQ is not implemented in this iteration.

## Scope

The scope of this iteration is limited to:

- Hash join.
- Non-empty `left_join_keys/right_join_keys`.
- NullEQ appearing only on join keys.

The following cases are not included:

- Cartesian join.
- Plan shapes where NullEQ join semantics are expressed only through `other_conditions`.
- Plans where the planner rewrites `<=>` into another expression and expects TiFlash to
  infer the original semantics.

## Input Contract

### tipb Protocol

The recommended change is to add the following field to `tipb::Join`:

- `repeated bool is_null_eq = ...;`

The semantics are:

- `is_null_eq[i] = false`: the `i`-th join-key pair uses ordinary `=`.
- `is_null_eq[i] = true`: the `i`-th join-key pair uses `<=>`.

Length constraints:

- `is_null_eq_size == 0`: treat all entries as `false` for backward compatibility.
- Otherwise, all of the following must hold:
  - `is_null_eq_size == left_join_keys_size`.
  - `is_null_eq_size == right_join_keys_size`.

### Join-Key Representation

The MVP assumes that the planner sends join keys as column references:

- `left_join_keys[i]` and `right_join_keys[i]` are aligned key pairs.
- TiFlash may insert casts in the execution layer to align types, but must not change the
  order or number of keys.
- `is_null_eq[i]` is always aligned by key-pair index, not by the build/probe role.

### Semantic Boundary

NullEQ semantics are expressed only through `is_null_eq[]`:

- If `<=>` appears in `other_conditions`, it is handled as an ordinary boolean expression.
  The execution layer is not required to infer join-key NullEQ from `<=>` inside
  `other_conditions`.
- The execution layer must not infer whether a key is NullEQ from a generic expression.

### Relationship with NullAware Join

`is_null_aware_semi_join` and NullEQ represent different semantics:

- NullAware join handles three-valued logic for `NOT IN`.
- NullEQ handles comparison semantics for join keys.

The MVP recommends the following behavior:

- If `is_null_aware_semi_join=true` and any `is_null_eq[i]=true`, fail fast.

Both paths make strong assumptions about how rows with `NULL` keys are handled, so mixing
them can easily produce silent wrong results.

## Key Assumptions in the Existing Implementation

There are four main assumptions in the current join framework that directly conflict with
NullEQ.

### 1. Key-NULL Rows Are Filtered Early

The current build and probe paths process nullable keys in two steps:

1. Replace `ColumnNullable` with its nested column.
2. Write rows containing `NULL` keys into `null_map`.

The relevant paths are:

- Build: `Join::insertFromBlockInternal()`.
- Probe: `ProbeProcessInfo::prepareForHashProbe()`.

This means:

- Build-side rows with `NULL` keys are not inserted into the map by default.
- Probe-side rows with `NULL` keys do not probe the map by default.

That directly conflicts with the fact that `NULL <=> NULL` must be matchable.

### 2. Side Conditions and Key-NULL Rows Share One null_map

`recordFilteredRows()` currently reuses the same `null_map` to combine side-condition
results with the information about whether a key is `NULL`.

The problem for NullEQ is not that the execution path must permanently maintain two
independent maps. The problem is that these two sources must be distinguished when the
final filter result is generated:

- A `NULL` in an ordinary `=` key must be written to the final filter result.
- A `NULL` in a NullEQ key must not be written to the final filter result.
- Side-condition failures must always be written to the final filter result.

A more accurate approach is:

- Decide at key granularity which `NULL` values should participate in filtering.
- Merge those results with side-condition results into one unified `row_filter_map`.

Therefore, the final implementation may still maintain only one map indicating whether a
row should be skipped, but it must not continue using the current approach of writing all
key-NULL rows into `null_map` before adding side-condition results.

### 3. RowsNotInsertToMap and Scan-After-Probe Treat NULL Keys as Naturally Unmatched

For join kinds that need to preserve special build-side rows, such as right/full/right
semi/right anti/null-aware joins, the current implementation records rows not inserted
into the map in `RowsNotInsertToMap`, then outputs them during scan-after-probe.

This is valid under ordinary `=` semantics because rows with `NULL` keys do not match.

Under NullEQ semantics:

- A row with a `NULL` key is not necessarily unmatched.
- It may need to enter the map and match a probe-side row with a `NULL` key.

### 4. KeyGetter Does Not Encode the Nullable Bitmap by Default

Fixed-key hash methods such as `keys128/keys256` currently default to
`has_nullable_keys = false`.

Consequently, even if `NULL` rows are not filtered early, the existing packed-key path
may still fail to encode nullness into the hash key correctly.

## Additional Interaction with FULL OUTER JOIN

NullEQ is not specific to `FULL OUTER JOIN`; `LEFT OUTER JOIN` and `RIGHT OUTER JOIN`
are affected as well. However, after TiFlash added `FULL OUTER JOIN`, several interactions
must be made explicit in the design. Otherwise, both sides can produce incorrect results.

### 1. The Natural-Unmatched NULL-Key Path Is Not Full-Join Specific, but FULL Amplifies It

The impact differs by join type:

- `LEFT OUTER JOIN`
  - If a probe-side `NULL` key still goes directly through `addNotFound()`, a row that
    should match through `NULL <=> NULL` is incorrectly emitted as left unmatched.
- `RIGHT OUTER JOIN`
  - If a build-side `NULL` key still enters `RowsNotInsertToMap`, a row that should match
    is incorrectly emitted as right unmatched during scan-after-probe.
- `FULL OUTER JOIN`
  - Both paths are present at the same time.
  - A group of rows that should match through `NULL <=> NULL` may be incorrectly split into:
    - one left-unmatched row;
    - one right-unmatched row.

This is not a full-join-only problem, but FULL makes the symptom most obvious and the
resulting behavior most complex.

### 2. FULL + Other Condition Must Continue Using Delayed setUsed Semantics

The current full-join path has a dedicated correction for `full + other condition`:

- Do not call `setUsed()` immediately when the join key matches.
- Mark the build row as used only after the other condition actually passes.

Otherwise, scan-after-probe may omit a build row that should be emitted as unmatched.

After introducing NullEQ, this constraint still applies to `NULL <=> NULL` matches:

- The key matches because `NULL <=> NULL`.
- The other condition fails.
- The correct result is:
  - keep one left-unmatched row with right-side NULLs;
  - emit the build row as unmatched during the later scan.

Therefore, NullEQ must continue using the existing row-flagged and delayed-used design
for the full-join path.

### 3. RowsNotInsertToMap Must Be Redefined for FULL

After FULL support, `RowsNotInsertToMap` can no longer simply mean "all NULL-key rows
plus all rows that failed the build condition".

Its more precise meaning should be:

- rows that failed a build-side condition;
- rows filtered because an ordinary `=` key contained `NULL`.

It must not include:

- rows whose NullEQ key is `NULL`.

Those rows should enter the map and may match successfully.

### 4. Dispatch Hash, Spill, and Fine-Grained Shuffle Expose More Errors Under FULL

If dispatch hashing does not encode nullness into the key:

- Build-side `NULL` keys and probe-side `NULL` keys may be sent to different partitions.
- An inner join usually appears simply as a missing match.
- Under FULL, it may additionally become:
  - one unmatched row from the probe side;
  - one unmatched row from the build-side scan.

Therefore, `full + NullEQ + spill/FGS` must be part of the MVP test matrix rather than
being deferred to a later iteration.

### 5. FULL Schema Nullability Does Not Need a Separate NullEQ Extension

No additional complexity is needed here:

- Both sides of a FULL output schema should already be nullable.
- Input schemas for other conditions are already made nullable according to FULL semantics.

NullEQ changes the **matching semantics**, not the nullable rules for the FULL output schema.

## Design Choices

## 1. General Principles

The NullEQ design follows two core principles:

1. Separate "whether a key is NULL" from "whether a row is filtered by a side condition".
2. Make the `NULL` value of a NullEQ key participate in key comparison instead of treating it
   as a special unmatched row.

This leads to two concepts:

- `row_filter_map`
  - Indicates that a row should not be inserted or probed.
  - The reason may be a failed left/right condition or a `NULL` in an ordinary `=` key.
- `key_null_map`
  - Meaningful only for ordinary `=` keys.
  - A NullEQ key must not write `NULL` into this map.

The names are retained to describe the source of each filtering result.

In the final implementation, these concepts may be represented by one unified
`row_filter_map`:

- `NULL` in an ordinary `=` key may be written to this map.
- Left/right side-condition results may also be written to this map.
- `NULL` in a NullEQ key must not be written to this map.

In other words, the key requirement is not that two independent maps must always be
maintained. The key requirement is that generating the final filter result must determine
at key granularity which `NULL` values mean "skip insert/probe".

## 2. Distinguish `=` and `<=>` by Key

For each key pair:

- If `is_null_eq[i] = false`:
  - Preserve the existing `=` semantics.
  - A `NULL` in any key component means that the row does not participate in matching.
- If `is_null_eq[i] = true`:
  - Preserve the nullable key.
  - `NULL` may participate in hashing, probing, and matching.

NullEQ does not make the entire join null-safe. It takes effect independently for each
key pair.

## 3. Build-Path Design

The build path must satisfy the following:

- A row with a `NULL` NullEQ key may enter the map.
- A row with a `NULL` ordinary `=` key must not enter the map.
- A row that fails a side condition must not enter the map, while any fallback output
  required by outer-join semantics must still be preserved.

Recommended approach:

1. Start from the original key columns instead of unconditionally calling
   `extractNestedColumnsAndNullMap()` for every key.
2. Process each key:
   - For an `=` key:
     - If it is nullable, use its nested column.
     - OR its null map into `row_filter_map`.
   - For a `<=>` key:
     - Preserve `ColumnNullable`.
     - Do not write its null map into `row_filter_map`.
3. OR build-side condition results into `row_filter_map`.
4. Pass `row_filter_map` to `JoinPartition::insertBlockIntoMaps(..., row_filter_map, ...)`.

The build-path semantics then become:

- `row_filter_map[i] = 1`: do not insert this row into the map.
- `row_filter_map[i] = 0`: insert this row into the map.

For join kinds that record special build-side rows, such as full/right outer, right semi,
and right anti:

- Only rows that failed a side condition or contain `NULL` in an ordinary `=` key should
  enter `RowsNotInsertToMap`.
- Rows with `NULL` in a NullEQ key must not enter `RowsNotInsertToMap`.

## 4. Probe-Path Design

The probe path must satisfy the following:

- A row with a `NULL` NullEQ key may actually probe the map.
- A row with a `NULL` ordinary `=` key remains a non-matching row.
- Fallback output for probe-side unmatched rows remains correct for left/full outer semantics.

The recommended approach mirrors the build path:

1. Do not unconditionally call `extractNestedColumnsAndNullMap()` for every key.
2. Process each key:
   - Write `NULL` from an `=` key into `row_filter_map`.
   - Preserve a nullable `<=>` key and do not write its `NULL` into `row_filter_map`.
3. OR probe-side condition results into `row_filter_map`.
4. During probing:
   - Rows with `row_filter_map[i] = 1` continue through the historical unmatched path.
   - Rows with `row_filter_map[i] = 0` actually probe the hash map.

The effects on outer joins are:

- `LEFT OUTER JOIN` no longer prematurely treats a probe-side NullEQ `NULL` row as unmatched.
- `FULL OUTER JOIN` follows the same rule, while also aligning with build-side
  scan-after-probe unmatched semantics.

## 5. Hash-Key Encoding Strategy

### Current Implementation

When a nullable NullEQ key is present, the current Join map-method selection is:

- If all participating key columns are fixed-size and the `null bitmap + payload` fits in
  `UInt128/UInt256`:
  - use `JoinMapMethod::nullable_keys128` or `JoinMapMethod::nullable_keys256`;
- otherwise, fall back to `JoinMapMethod::serialized`.

The fixed-size path reuses the nullable packed-key approach already used by HashAgg/Set:

- `keys128/keys256 + has_nullable_keys = true`;
- encode the nullness bitmap together with the key payload in the packed key.

This means common nullable numeric/datetime NullEQ joins do not always need to fall back
to `serialized`.

`serialized` remains the correctness fallback:

- Variable-length keys can naturally preserve the nullness of `ColumnNullable`.
- Fixed-size keys whose bitmap does not fit in `UInt256` can still use `serialized`.

There is one prerequisite that must be satisfied explicitly:

- `serialized` preserves nullness based on the current column object.
- It does not automatically normalize `Nullable(T)` and `T` to the same physical encoding.

`ColumnNullable::serializeValueIntoArena()` writes a null flag first and then the nested
value. Therefore, for the same non-NULL value:

- the serialized representation of `Nullable(Int32)`;
- and the serialized representation of `Int32`;

are different.

This means that if one side of a NullEQ key pair is nullable and the other is non-nullable,
both sides may use `serialized` and still fail to match, as long as their final key schemas
remain `Nullable(T)` and `T`.

Therefore, the MVP cannot merely force nullable NullEQ keys to `serialized`. It must also
ensure that:

- for each `is_null_eq[i] = true` key pair;
- whenever either side needs to preserve nullable semantics;
- both build and probe sides are aligned to the same physical key schema during key preparation;
- the most direct approach is to normalize both sides to `Nullable(common_type)`.

This schema-alignment requirement was initially part of the `serialized` correctness fallback.
It remains necessary after the fixed-size packed-key optimization is introduced.

## 6. JoinPartition / KeyGetter Semantics

When implementing NullEQ in JoinPartition, the important question is not simply whether
the column is nullable. The key getter must be able to encode nullness into the key.

The current JoinPartition has explicit nullable-aware fixed-key KeyGetter branches:

- `nullable_keys128 -> HashMethodKeysFixed<..., UInt128, ..., true, false>`;
- `nullable_keys256 -> HashMethodKeysFixed<..., UInt256, ..., true, false>`.

The semantics are:

- The packed-key path includes nullness in the key.
- Variable-length keys or fixed-size keys that exceed `UInt256` continue to use `serialized`.

Regardless of the selected path, the following must hold:

- Key columns passed by build and probe preserve nullable information for NullEQ keys.
- For every NullEQ key pair, the final key schemas used by build and probe are identical.
- In particular, mixed nullable/non-nullable cases must not remain as `Nullable(T)` versus `T`.

## 7. FULL + Other Condition Semantics

Because the full-join path already has row-flagged logic, NullEQ does not need a separate
FULL semantic path. It must ensure that a NullEQ key match goes through the existing correct
path:

1. For `full + other condition`:
   - continue using the row-flagged map;
2. If the key matches but the other condition fails:
   - delay marking the build row as used until the other condition passes;
3. Apply this rule to:
   - ordinary-value matches;
   - `NULL <=> NULL` matches.

Otherwise, FULL may omit a build row or emit duplicate unmatched rows.

## 8. RuntimeFilter

The MVP recommends:

- Disable runtime filters whenever the join contains NullEQ and at least one nullable
  NullEQ key.

The reason is that the current runtime-filter/Set path still drops `NULL` keys by default,
which conflicts with the NullEQ rule that `NULL` may match.

A possible long-term direction is:

- maintain an additional `has_null` flag in Set;
- apply a single-column NullEQ runtime filter as:
  - `isNull(x) ? has_null : (x IN set)`.

This is not recommended for the MVP.

## 9. Alternative: Rewrite `<=>` in the Planner

Another approach is for the TiDB planner not to send explicit `is_null_eq[]`, but to rewrite
each `<=>` key into:

1. `isNull(k)`;
2. `ifNull(k, sentinel)`.

TiFlash could then continue using ordinary `=` joins.

The advantage is that execution-layer changes would be smaller. The disadvantages are clear:

- Every `<=>` key becomes two keys.
- Hash keys become wider.
- Planner, runtime filter, cast, and collation handling become more complicated.
- Key-level semantics become less explicit.

Therefore, this design chooses native key-level NullEQ support in TiFlash.

## Testing Recommendations

The MVP should cover at least:

1. `INNER JOIN`:
   - `NULL <=> NULL` matches;
   - `NULL <=> 1` does not match.
2. `LEFT OUTER JOIN`:
   - A probe-side `NULL` key is not prematurely treated as unmatched.
3. `RIGHT OUTER JOIN`:
   - A build-side `NULL` key is not incorrectly placed in `RowsNotInsertToMap`.
4. `FULL OUTER JOIN`:
   - A `NULL <=> NULL` match is not split into two unmatched rows.
   - If a `NULL <=> NULL` key match fails the other condition, both unmatched sides are correct.
5. `SEMI / ANTI`:
   - `NULL <=> NULL` participates in existence checks.
6. Multiple keys with mixed semantics:
   - `k1 <=> k1 AND k2 = k2`;
   - `k1 <=> k1 AND k2 <=> k2`.
7. Side-condition interaction:
   - left/right conditions coexist with NullEQ keys.
8. Spill / fine-grained shuffle:
   - especially `FULL OUTER JOIN + NullEQ`.

### CP3 Test Progress

Based on the current workspace progress, the CP3 spill/FGS paths are covered:

1. `spill + FULL OUTER JOIN + NullEQ`:
   - A `NULL <=> NULL` match is not split into two unmatched rows.
2. `spill + FULL OUTER JOIN + NullEQ + other condition`:
   - Data covers both `other condition = false` and `other condition = true`.
   - When `other condition = false`, the build row is still emitted correctly during
     scan-after-probe.
   - When `other condition = true`, the build row is consumed normally and is not emitted again.
3. `fine-grained shuffle + NullEQ`:
   - Covers a nullable key.
   - Verifies that after build/probe key-schema alignment, the probe does not incorrectly
     classify a NullEQ `NULL` as filtered or unmatched.

## Code Hotspots

- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.*`
- `dbms/src/Flash/Planner/Plans/PhysicalJoin.cpp`
- `dbms/src/Interpreters/Join.h`
- `dbms/src/Interpreters/Join.cpp`
- `dbms/src/Interpreters/ProbeProcessInfo.cpp`
- `dbms/src/Interpreters/JoinPartition.cpp`
- `dbms/src/Interpreters/JoinHashMap.cpp`
- `dbms/src/DataStreams/ScanHashMapAfterProbeBlockInputStream.cpp`
- `dbms/src/Interpreters/Set.cpp`

---

## Development Tracking / Dev Note

This section is placed in the latter half of the document to record implementation progress
as development proceeds by checkpoint. The design conclusions in the preceding sections take
priority.

### How to Continue

Before continuing development, it is recommended to always do three things:

1. Read the design section of this document first.
2. Run `git status` / `git diff --stat`.
3. Explicitly identify which checkpoint this change is intended to advance.

Suggested wording for future instructions:

- "Use `docs/note/nulleq_join.md` as the source of truth and continue from CP2."
- "Read the design document first, then read the current progress."

### Milestones

#### Milestone 0: Protocol / Plumbing

Done criteria:

- TiFlash can parse `is_null_eq[]`.
- The value is passed through to `DB::Join`.
- Behavior is unchanged when the field is not sent.

#### Milestone 1: Correctness MVP

Done criteria:

- Nullable NullEQ keys build and probe correctly.
- Mixed nullable/non-nullable NullEQ key pairs align their key schemas and match correctly.
- Outer join and scan-after-probe do not misclassify NullEQ `NULL` rows as unmatched.
- `FULL OUTER JOIN + other condition` has correct combined semantics with NullEQ.
- Runtime filters are disabled in this mode.

#### Milestone 2: Test Matrix

Done criteria:

- The basic inner/left/right/full/semi/anti matrix is covered.
- Mixed keys, side conditions, and spill/FGS are covered.

#### Milestone 3: Performance Optimization

Done criteria:

- Nullable fixed-size keys no longer always fall back to `serialized`.

#### Milestone 4: RuntimeFilter (Optional)

Done criteria:

- Runtime-filter semantics for a single-column NullEQ key are correct, or long-term
  disabling is explicitly documented.

### Suggested Checkpoints

- CP0: tipb field and TiFlash parsing.
- CP1: `DB::Join` stores and logs `is_null_eq`.
- CP2.1: force nullable NullEQ keys to `serialized`, align mixed-nullability key schemas,
  and add the NullAware mutual-exclusion check.
- CP2.2: split the build/probe `row_filter_map` semantics.
- CP2.3: adjust `RowsNotInsertToMap` and scan-after-probe.
- CP2.4: validate the interaction between `FULL OUTER JOIN + other condition` and NullEQ.
- CP2.5: disable runtime filters for the MVP.
- CP3: add tests.
- CP4: optimize packed keys.

### Current Progress

- Note: the following checklist was verified against the current workspace and records the
  progress of this development iteration.
- [x] tipb: `Join.is_null_eq` field definition.
- [x] TiFlash: `JoinInterpreterHelper::TiFlashJoin` parses `is_null_eq[]`.
- [x] TiFlash: `DB::Join` stores and logs `is_null_eq`.
- [x] TiFlash: force nullable NullEQ keys to `serialized`, align mixed-nullability key
  schemas, and fail fast for NullAware conflicts.
- [x] TiFlash: split the build/probe `row_filter_map` semantics.
- [x] TiFlash: adjust `RowsNotInsertToMap` and scan-after-probe.
- [x] TiFlash: validate `FULL OUTER JOIN + other condition` with NullEQ.
- [x] TiFlash: disable runtime filters.
- [x] TiFlash: gtest covers the basic inner/left/right/full/semi/anti matrix.
- [x] TiFlash: gtest covers mixed keys and side-condition interaction.
- [x] TiFlash: spill/fine-grained shuffle coverage.
- [x] TiFlash: packed-key optimization (nullable fixed-size NullEQ keys may use
  `nullable_keys128/256`; other cases fall back to `serialized`).

### Open Questions

- When will TiDB/kvproto synchronize `is_null_eq[]`?
- If expressions are allowed as keys in the future, how will `is_null_eq[i]` remain aligned?
- Is the performance fallback for strings and collations acceptable?
- Are separate profile or debug metrics needed for spill/FGS cases?
- Will NullAware join always be mutually exclusive with NullEQ, or should combined semantics
  be defined in the future?
