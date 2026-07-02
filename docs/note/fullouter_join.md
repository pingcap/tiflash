# TiFlash FULL OUTER JOIN Support Change List (Equi Join Only in This Round)

## Background

The TiFlash kernel, which inherits the Join architecture from ClickHouse, already has basic support for `ASTTableJoin::Kind::Full`. However, because TiDB has not pushed down full outer join for a long time, this path has not been continuously covered. In particular, the `other condition` logic added later mostly covered left/right outer joins only.

TiDB is now preparing to support full outer join. After it is pushed down to TiFlash, we need to complete the protocol mapping, output schema, execution semantics, and test coverage together.

The scope of this round is explicitly limited to `FULL OUTER JOIN` with non-empty `left_join_keys/right_join_keys`, which means the hash join path with equi join keys.

## Current Conclusions

### Existing Capabilities That Can Be Reused

1. The SQL/AST layer already recognizes Full.
- `dbms/src/Parsers/ParserTablesInSelectQuery.cpp:117`
- `dbms/src/Parsers/ASTTablesInSelectQuery.cpp:165`

2. In the hash join framework, `Full` is already treated as a join that needs to scan unmatched build-side rows after probing.
- `dbms/src/Interpreters/JoinUtils.h:26`
- `dbms/src/Interpreters/JoinUtils.h:84`

3. The basic full path without `other condition` mostly exists.
- Nullable handling for probe-side columns: `dbms/src/Interpreters/ProbeProcessInfo.cpp:71`
- Nullable handling for build-side sample columns: `dbms/src/Interpreters/Join.cpp:346`
- Nullable handling for build blocks: `dbms/src/Interpreters/Join.cpp:696`
- Full uses `MapsAllFull` during probing: `dbms/src/Interpreters/JoinPartition.cpp:2124`

### Main Gaps That Must Be Filled

1. The TiDB DAG protocol and TiFlash JoinType mapping do not have full yet.
- `contrib/tipb/proto/executor.proto:184`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:68`

2. The nullable rules for Full output/intermediate schemas are incomplete. They currently only handle left/right outer joins.
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:298`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:333`
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:175`

3. Correctness for `full + other condition` is currently broken.
- `handleOtherConditions` has no full branch and may directly throw a logical error: `dbms/src/Interpreters/Join.cpp:1032`
- Full currently uses `MapsAllFull`. When a key is hit, it calls `setUsed()` too early. If `other condition` filters the row out later, TiFlash cannot restore the "unmatched right row" state: `dbms/src/Interpreters/JoinPartition.cpp:1601`

4. The current left/right condition validation does not allow full.
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h:97`

5. Full with no equi key, where join keys are empty, is out of scope for this round. Cartesian full join will not be enabled in this round.

## Detailed Changes

## 1. Protocol and JoinType Mapping

1. Add `TypeFullOuterJoin` to tipb, synchronized with the final protocol value from TiDB.
- Location: `contrib/tipb/proto/executor.proto`

2. Add the full mapping in `JoinInterpreterHelper::getJoinKindAndBuildSideIndex`.
- Equal join with keys should at least support:
  - `{TypeFullOuterJoin, 0} -> {ASTTableJoin::Kind::Full, 0}`
  - `{TypeFullOuterJoin, 1} -> {ASTTableJoin::Kind::Full, 1}`
- Convention, consistent with TiDB tests: the build side of `FULL OUTER JOIN` is specified directly by `inner_idx`, that is, `build_side_index == inner_idx`.
- Note: the full case does not need to adjust `join kind` like left/right outer join does. The execution layer will still wire the probe/build roles according to `build_side_index` to satisfy TiFlash's internal right-build convention.

3. Add full to stringification/logging branches to avoid falling into the default error path.
- `dbms/src/Flash/Coprocessor/DAGUtils.cpp:837`

## 2. Nullable/Schema Rules for Full

1. Join output schema: both left-side and right-side columns must be nullable for full.
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:333`

2. Input schema for compiling `other condition`: for full, both sides, plus the extra columns added during probe preparation, must follow full semantics and become nullable as needed.
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:298`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:313`

3. `collectOutputFieldTypes` must also make both sides nullable for externally visible field types.
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:175`
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:213`

## 3. Validation for Left/Right Conditions Under Full

`JoinNonEqualConditions::validate` currently limits left condition to left outer join and right condition to right outer join. Under full, both kinds of conditions may appear and should be allowed.

- Location: `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h:95`

## 4. Core Correctness: Full + Other Condition

This is the most important change in this round.

Current problem:

1. During probing, full uses `MapsAllFull` and calls `setUsed()` as soon as the key is hit.
2. `other condition` may filter out those rows afterward.
3. However, the build-side "used" mark has already been set, so the post-probe scan will no longer output those records, even though they should be output as "unmatched right rows".

The required behavior is: `used` can only be set after `other condition` passes.

Implementation-wise, the following items 1-6 can conceptually be split into "mark timing", "`handleOtherConditions` semantics", and "post-probe scan integration". However, the current code paths are tightly coupled:

1. If we only switch to row-flagged maps without adding a full branch in `handleOtherConditions`, `full + other condition` may still hit a runtime error branch.
2. If we only switch the probe-side map without updating the post-probe scan branch, the scan phase will still read the wrong hash map type.

Therefore, the following items 1-6 should at least be delivered as one review/commit unit. Steps 6/7/8 later in this document remain as conceptual decomposition for easier verification, but they should not be mechanically split into three independent patches that can be merged separately.

Suggested implementation direction:

1. Make full use row-flagged maps (`MapsAllFullWithRowFlag`) when `has_other_condition=true`.
- Affected locations:
  - `dbms/src/Interpreters/JoinUtils.h:89`
  - `dbms/src/Interpreters/JoinPartition.cpp:281`
  - `dbms/src/Interpreters/JoinPartition.cpp:971`
  - `dbms/src/Interpreters/JoinPartition.cpp:1037`

2. Add a full + row_flagged dispatch branch in `JoinPartition::probeBlock`.
- Full currently always uses `MapsAllFull`: `dbms/src/Interpreters/JoinPartition.cpp:2124`

3. The row-flagged probe adder needs to support the "fallback left output" semantics of full.
- Currently `RowFlaggedHashMapAdder::addNotFound` does not append a default row: `dbms/src/Interpreters/JoinPartition.cpp:1450`
- For full, when not-found happens, it still needs to output one row with right-side defaults and write nullable values into the helper column.

4. Add a full branch in `Join::handleOtherConditions` with semantics aligned to the fallback behavior of left outer join.
- Currently the "keep at least one row + set right side to null" logic only runs when `isLeftOuterJoin(kind)`:
  - `dbms/src/Interpreters/Join.cpp:999`
  - `dbms/src/Interpreters/Join.cpp:1017`

5. In `Join::doJoinBlockHash` for full+row_flagged:
- When marking build rows as used according to helper pointers, skip null pointers.
- Remove helper temporary columns before outputting the result.
- Related location: `dbms/src/Interpreters/Join.cpp:1325`

6. Switch the post-probe scan phase to the row_flagged branch for full+other_condition.
- `dbms/src/DataStreams/ScanHashMapAfterProbeBlockInputStream.cpp:256`

## 5. Full Without Equi Keys (Cartesian Full) Is Out of Scope

The current code does not have a complete execution path for full without keys, and this round does not enable that path. It is recommended to add an explicit guard to avoid accidentally taking an invalid path:

1. If TiFlash receives `TypeFullOuterJoin` with empty join keys, explicitly return `Unimplemented/BadRequest`, and make the error message clear that "cartesian full is not supported".
2. If support is needed later, implement the cross full path as a separate task. The workload is clearly larger than equal full.

It is recommended to reject this case in `getJoinKindAndBuildSideIndex` or further upstream, instead of letting it fall into unclear errors such as "Unknown join type".

## 6. Debug/Mock and Test Construction Path

To make gtests able to construct full join DAGs, the mock binder also needs to be updated.

1. Add full to mock schema nullable rules, making both sides nullable.
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:99`
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:296`

2. Add full to the old AST -> tipb test compilation path.
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:384`

## 7. Test Change Suggestions

Minimum recommended coverage:

1. Coprocessor mapping tests
- `dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`
- Add full + `inner_idx=0/1` with keys.
- Explicitly assert `build_side_index == inner_idx` for full.
- Optionally add one no-key rejection case to verify the error message, without implementing the execution logic.

2. Join Executor correctness
- Refer to the existing construction pattern for `RightOuterJoin`: `dbms/src/Flash/tests/gtest_join_executor.cpp:4031`
- Focus on adding full cases:
  - With keys, without other condition
  - With keys, with other condition
  - Key hit but all rows fail `other condition` (must output both left-unmatched and right-unmatched rows)
  - With left/right conditions
  - With null keys

3. Spill / fine-grained shuffle
- Add at least one full + other condition spill case.
- Existing join type arrays still contain only seven join types:
  - `dbms/src/Flash/tests/gtest_join.h:184`
  - `dbms/src/Flash/tests/gtest_compute_server.cpp:1248`
- It is not recommended to expand all full arrays to eight join types and rewrite many expected results immediately. Add targeted full cases first.

## Suggested Implementation Order

1. Sync tipb and get `TypeFullOuterJoin`.
2. Enable JoinType mapping, schema nullable handling, and `getJoinTypeName`.
3. Allow left/right condition validation for full.
4. Implement the main correctness path for `full + other condition` together: row-flagged path, `handleOtherConditions`, and post-probe scan integration.
5. Add an explicit rejection for full without equi keys, which is not implemented in this round.
6. Add gtests, starting with targeted cases before considering expanding the join type matrix.

## Development Steps (Executable Checklist)

1. Step 1: enable the protocol enum, only full and only with equi keys
- Goal: TiFlash can recognize `TypeFullOuterJoin`.
- Changes:
  - Add `TypeFullOuterJoin` to `contrib/tipb/proto/executor.proto`.
  - Generate/sync the corresponding protobuf code according to the repository's existing workflow.
- Acceptance:
  - The code compiles without `JoinType` enum errors.
  - `TypeFullOuterJoin` can be referenced in TiFlash code.

2. Step 2: JoinType mapping and build-side convention
- Goal: map full to `ASTTableJoin::Kind::Full`, with `build_side_index == inner_idx`.
- Changes:
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp`
  - `dbms/src/Flash/Coprocessor/DAGUtils.cpp` (`getJoinTypeName`)
  - `dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`, if needed
- Acceptance:
  - full + `inner_idx=0/1` both return `kind=Full`, and `build_side_index` is equal to `inner_idx`.

3. Step 3: equi-key scope guard, explicitly reject no-key full
- Goal: cartesian full is not implemented in this round, and TiFlash returns a clear error for this request.
- Changes:
  - Prefer adding the guard in `JoinInterpreterHelper::getJoinKindAndBuildSideIndex(...)` or further upstream.
- Acceptance:
  - When `TypeFullOuterJoin` has `join_keys_size==0`, the error message clearly says cartesian full is not supported.

4. Step 4: make output and other-condition input schemas fully nullable for full
- Goal: under full, both left-side and right-side output columns, and the columns used to compile other-condition, are treated as nullable.
- Changes:
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp`
  - `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp`
  - `dbms/src/Debug/MockExecutor/JoinBinder.cpp` for the test construction path
- Acceptance:
  - In full output schema, columns from both sides are nullable, without affecting the semi join family.

5. Step 5: allow left/right condition validation for full
- Goal: full can carry left/right conditions and will not be rejected by `validate`.
- Changes:
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h` (`JoinNonEqualConditions::validate`)
- Acceptance:
  - Constructing full + left_condition and full + right_condition does not report "non left/right join with ... conditions".

6. Step 6: core change A - used mark timing for full + other condition (recommended to deliver together with steps 7 and 8)
- Goal: mark build rows as used only after `other condition` passes, so unmatched right rows are not lost.
- Notes:
  - In the current code, row-flagged map selection, full semantics in `handleOtherConditions`, and the post-probe scan branch are tightly coupled.
  - If only this step is applied without steps 7 and 8, the code may partially compile, but `full + other condition` still cannot form a runnable and verifiable complete path.
- Changes:
  - `dbms/src/Interpreters/JoinUtils.h` to make full+other_condition enter the row-flagged path
  - `dbms/src/Interpreters/JoinPartition.cpp` for map initialization, probe dispatch, and adder not-found behavior
  - `dbms/src/Interpreters/Join.cpp` to skip null pointers when marking used rows in `doJoinBlockHash`
- Acceptance:
  - In the case where keys hit but all rows fail `other condition`, right-side rows can still be output during the post-probe scan.

7. Step 7: core change B - full semantics in `handleOtherConditions` (usually delivered together with step 6)
- Goal: full also needs the correct "outer join keeps at least one row + set right side to null" behavior.
- Changes:
  - `dbms/src/Interpreters/Join.cpp` (`handleOtherConditions` branch)
- Acceptance:
  - Results of full + other_condition are consistent with the symmetric combination semantics of left/right outer join.

8. Step 8: integrate the post-probe scan phase (usually delivered together with step 6)
- Goal: full+other_condition uses the row-flagged map to scan unmatched right rows.
- Changes:
  - `dbms/src/DataStreams/ScanHashMapAfterProbeBlockInputStream.cpp`
- Acceptance:
  - full+other_condition can correctly output unmatched build-side rows without missing or duplicating rows.

9. Step 9: add tests, starting with targeted cases
- Goal: guarantee correctness first, then expand matrices if needed.
- Suggested changes:
  - `dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`
  - `dbms/src/Flash/tests/gtest_join_executor.cpp`
  - `dbms/src/Flash/tests/gtest_spill_join.cpp` (at least one case)
- Minimum acceptance set:
  - full + key + no other_condition
  - full + key + with other_condition
  - full + key hit but all rows fail other_condition (critical regression)
  - full + left/right conditions
  - full + null key

## One-Line Risk Summary

If we only implement "JoinType mapping + nullable output" without changing the row-flagged logic, `FULL OUTER JOIN ... ON eq_key AND other_condition` will miss right-side rows. This is a result-correctness bug, not a performance issue, and must be completed together with protocol enablement.
