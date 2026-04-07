# TiFlash 支持 FULL OUTER JOIN 改动点梳理（本轮仅等值 Join）

## 背景

TiFlash 内核（继承自 ClickHouse 的 Join 架构）本身有 `ASTTableJoin::Kind::Full` 的基础能力，但在 TiDB 长期不下推 full outer join 的情况下，这条路径没有被持续覆盖，尤其是后续补上的 `other condition` 逻辑基本只覆盖了 left/right outer。

现在 TiDB 侧准备支持 full outer join，下推到 TiFlash 后，需要把协议映射、输出 schema、执行语义和测试覆盖一起补齐。

本文档本轮 scope 明确限定为：`FULL OUTER JOIN` 且 `left_join_keys/right_join_keys` 非空（即有等值 join key 的 hash join 路径）。

## 现状结论

### 已有能力（可复用）

1. SQL/AST 层已识别 Full。
- `dbms/src/Parsers/ParserTablesInSelectQuery.cpp:117`
- `dbms/src/Parsers/ASTTablesInSelectQuery.cpp:165`

2. Hash Join 框架里，`Full` 已被视为需要“probe 后扫描 build 侧未匹配行”的 join。
- `dbms/src/Interpreters/JoinUtils.h:26`
- `dbms/src/Interpreters/JoinUtils.h:84`

3. 不带 `other condition` 的 full 基础路径基本存在。
- probe 侧列 nullable 处理：`dbms/src/Interpreters/ProbeProcessInfo.cpp:71`
- build 侧样本列 nullable 处理：`dbms/src/Interpreters/Join.cpp:346`
- build block nullable 处理：`dbms/src/Interpreters/Join.cpp:696`
- full 在 probe 时走 `MapsAllFull`：`dbms/src/Interpreters/JoinPartition.cpp:2124`

### 主要缺口（必须补）

1. TiDB DAG 协议和 TiFlash JoinType 映射还没有 full。
- `contrib/tipb/proto/executor.proto:184`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:68`

2. Full 的输出/中间 schema nullable 规则没有补齐（只处理 left/right outer）。
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:298`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:333`
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:175`

3. `full + other condition` correctness 当前不成立。
- `handleOtherConditions` 没有 full 分支，可能直接抛逻辑错误：`dbms/src/Interpreters/Join.cpp:1032`
- full 当前走 `MapsAllFull`，key 命中时会提前 `setUsed()`，other condition 过滤后无法恢复“未匹配右行”状态：`dbms/src/Interpreters/JoinPartition.cpp:1601`

4. left/right condition 校验目前不允许 full。
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h:97`

5. 无等值键（join keys 为空）的 full 当前不在本轮范围内（本轮先不打通 cartesian full）。

## 具体改动点

## 1. 协议与 JoinType 映射

1. 在 tipb 加 `TypeFullOuterJoin`（按 TiDB 最终协议值同步）。
- 位置：`contrib/tipb/proto/executor.proto`

2. `JoinInterpreterHelper::getJoinKindAndBuildSideIndex` 增加 full 映射。
- equal join（有 key）至少支持：
  - `{TypeFullOuterJoin, 0} -> {ASTTableJoin::Kind::Full, 0}`
  - `{TypeFullOuterJoin, 1} -> {ASTTableJoin::Kind::Full, 1}`
- 约定（与 TiDB 测试一致）：`FULL OUTER JOIN` 的 build side 由 `inner_idx` 直接指定（即 `build_side_index == inner_idx`）。
- 说明：full 场景不需要像 left/right outer 那样改 `join kind`；但执行层仍会按 `build_side_index` 做 probe/build 角色接线，以满足 TiFlash 内部 right-build 约定。

3. 字符串化/日志分支补 full，避免默认分支报错。
- `dbms/src/Flash/Coprocessor/DAGUtils.cpp:837`

## 2. Full 的 nullable/schema 规则

1. Join 输出 schema：full 需要左右两侧都 nullable。
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:333`

2. other condition 编译输入 schema：full 需要左右两侧（以及 probe prepare 新增列）按 full 语义 nullable。
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:298`
- `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp:313`

3. `collectOutputFieldTypes` 对外字段类型同样要把左右都改为 nullable。
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:175`
- `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp:213`

## 3. left/right condition 在 full 下的校验

`JoinNonEqualConditions::validate` 当前把 left condition 只限定在 left outer，把 right condition 只限定在 right outer；full 下这两类都可能出现，应允许。

- 位置：`dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h:95`

## 4. 核心正确性：full + other condition

这是本次最关键的改动。

当前问题：

1. full 在 probe 阶段使用 `MapsAllFull`，命中 key 就先 `setUsed()`。
2. 后续 `other condition` 再过滤时，可能把这些行过滤掉。
3. 但 build 侧“已使用”标记已经被置位，probe 后扫描阶段不会再输出这些本应作为“未匹配右行”的记录。

需要改成：`used` 只能在 `other condition` 通过后再设置。

实现上，下面 1-6 点虽然可以按职责拆成“标记时机 / `handleOtherConditions` 语义 / probe 后扫描联动”三个子问题，但当前代码路径存在强耦合：

1. 只切 row-flagged map，而不同时补 full 的 `handleOtherConditions` 分支，`full + other condition` 仍可能落到运行时错误分支。
2. 只切 probe 侧 map，而不同时补 probe 后扫描分支，后扫阶段仍会读取错误的 hash map 类型。

因此建议把下面 1-6 点至少作为同一个 review/提交单元交付；文档后面的第 6/7/8 步保留为概念拆分，便于逐项验收，但不建议机械地拆成三个互相独立、可单独合入的 patch。

建议实现方向：

1. 让 full 在 `has_other_condition=true` 时也走 row-flagged map（`MapsAllFullWithRowFlag`）。
- 受影响点：
  - `dbms/src/Interpreters/JoinUtils.h:89`
  - `dbms/src/Interpreters/JoinPartition.cpp:281`
  - `dbms/src/Interpreters/JoinPartition.cpp:971`
  - `dbms/src/Interpreters/JoinPartition.cpp:1037`

2. `JoinPartition::probeBlock` 增加 full + row_flagged 的 dispatch 分支。
- 当前 full 固定走 `MapsAllFull`：`dbms/src/Interpreters/JoinPartition.cpp:2124`

3. row-flagged probe adder 需要支持 full 的“左侧保底输出”语义。
- 现在 `RowFlaggedHashMapAdder::addNotFound` 不补默认行：`dbms/src/Interpreters/JoinPartition.cpp:1450`
- full 需要 not-found 时仍输出 1 行（右侧默认值）并给 helper 列写可判空值。

4. `Join::handleOtherConditions` 增加 full 分支，语义对齐 left outer 的保底行为。
- 当前只在 `isLeftOuterJoin(kind)` 时做“至少保留一行 + 右侧置 null”逻辑：
  - `dbms/src/Interpreters/Join.cpp:999`
  - `dbms/src/Interpreters/Join.cpp:1017`

5. `Join::doJoinBlockHash` 在 full+row_flagged 下：
- 根据 helper 指针给 build row 打 used 标记时要跳过空指针。
- 结果输出前移除 helper 临时列。
- 相关位置：`dbms/src/Interpreters/Join.cpp:1325`

6. probe 后扫描阶段对 full+other_condition 要切到 row_flagged 分支。
- `dbms/src/DataStreams/ScanHashMapAfterProbeBlockInputStream.cpp:256`

## 5. 无等值键 full（cartesian full）本轮不做

当前代码对 full 无 key 没有成型执行路径，但本轮按 scope 不打通该路径。建议只做明确保护，避免误走：

1. 若收到 `TypeFullOuterJoin` 且 join keys 为空，TiFlash 显式报 `Unimplemented/BadRequest`，错误信息写清楚“cartesian full 不支持”。
2. 后续如需支持，再单独立项实现 cross full 路径（工作量明显高于 equal full）。

建议在 `getJoinKindAndBuildSideIndex`（或更上游）就明确拦截，避免落到“Unknown join type”这类难排查报错。

## 6. Debug/Mock 与测试构造链路

为保证 gtest 能构造 full join DAG，需要同步补 mock binder。

1. mock schema nullable 规则补 full（左右都 nullable）。
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:99`
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:296`

2. AST -> tipb 的旧测试编译路径补 full。
- `dbms/src/Debug/MockExecutor/JoinBinder.cpp:384`

## 7. 测试改动建议

最低覆盖建议：

1. Coprocessor 映射测试
- `dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`
- 新增 full + `inner_idx=0/1`（有 key）
- 明确断言 `build_side_index == inner_idx`（full 场景）
- 可补 1 个无 key 的拒绝用例（验证报错信息），但不实现执行逻辑

2. Join Executor correctness
- 参考现有 `RightOuterJoin` 的构造模式：`dbms/src/Flash/tests/gtest_join_executor.cpp:4031`
- 重点新增 full 场景：
  - 有 key、无 other condition
  - 有 key、有 other condition
  - key 命中但 other condition 全失败（必须同时输出 left-unmatched 和 right-unmatched）
  - 含 left/right conditions
  - 含 null key

3. Spill / Fine-grained shuffle
- 至少补 1 组 full + other condition 的 spill case
- 现有 join type 数组都还是 7 种：
  - `dbms/src/Flash/tests/gtest_join.h:184`
  - `dbms/src/Flash/tests/gtest_compute_server.cpp:1248`
- 不建议直接把全量数组扩成 8 再重写大批 expected，可先加 targeted full 用例。

## 建议落地顺序

1. 同步 tipb（拿到 `TypeFullOuterJoin`）。
2. 打通 JoinType 映射 + schema nullable + `getJoinTypeName`。
3. 放开 full 的 left/right condition 校验。
4. 把 `full + other condition` 的 correctness 主链路一起打通（row-flagged 路径 + `handleOtherConditions` + probe 后扫描联动）。
5. 增加无等值键 full 的显式拒绝（本轮不实现）。
6. 补 gtest（先 targeted，再考虑扩大 join type 矩阵）。

## 开发步骤（可执行 checklist）

1. 第 1 步：协议枚举打通（仅 full，且仅等值 key）
- 目标：TiFlash 能识别 `TypeFullOuterJoin`。
- 修改：
  - `contrib/tipb/proto/executor.proto` 增加 `TypeFullOuterJoin`。
  - 生成/同步对应 protobuf 代码（按仓库现有流程）。
- 验收：
  - 编译无 `JoinType` 相关 enum 错误。
  - `TypeFullOuterJoin` 可在 TiFlash 代码中引用。

2. 第 2 步：JoinType 映射与 build side 约定
- 目标：full 映射到 `ASTTableJoin::Kind::Full`，并满足 `build_side_index == inner_idx`。
- 修改：
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp`
  - `dbms/src/Flash/Coprocessor/DAGUtils.cpp`（`getJoinTypeName`）
  - 若需要，`dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`
- 验收：
  - full + `inner_idx=0/1` 都能返回 `kind=Full` 且 `build_side_index` 与 `inner_idx` 一致。

3. 第 3 步：等值 key scope 保护（无 key 显式拒绝）
- 目标：本轮不实现 cartesian full，收到此类请求时报清晰错误。
- 修改：
  - 建议在 `JoinInterpreterHelper::getJoinKindAndBuildSideIndex(...)` 或更上游添加 guard。
- 验收：
  - `TypeFullOuterJoin` 且 `join_keys_size==0` 时，报错信息明确包含“不支持 cartesian full”。

4. 第 4 步：full 的输出与 other-condition 输入 schema 全量 nullable
- 目标：full 下左右输出列、other-condition 编译输入列都按 nullable 处理。
- 修改：
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.cpp`
  - `dbms/src/Flash/Coprocessor/collectOutputFieldTypes.cpp`
  - `dbms/src/Debug/MockExecutor/JoinBinder.cpp`（测试构造链路）
- 验收：
  - full 输出 schema 中左右列都是 nullable（不影响 semi 系列）。

5. 第 5 步：放开 full 的 left/right conditions 校验
- 目标：full 可带 left/right condition，不被 `validate` 拦截。
- 修改：
  - `dbms/src/Flash/Coprocessor/JoinInterpreterHelper.h`（`JoinNonEqualConditions::validate`）
- 验收：
  - full + left_condition、full + right_condition 的构造阶段不报 “non left/right join with ... conditions”。

6. 第 6 步：核心改造 A - full + other condition 的 used 标记时机（建议与第 7、8 步合并交付）
- 目标：只有当 `other condition` 通过时，build 行才标记 used，避免漏输出未匹配右行。
- 说明：
  - 当前代码上，row-flagged map 选择、`handleOtherConditions` 的 full 语义、probe 后扫描分支是强耦合的。
  - 如果只落本步、不同时补第 7 和第 8 步，代码可能虽然能部分编译，但 `full + other condition` 仍无法形成可运行、可验证的完整链路。
- 修改：
  - `dbms/src/Interpreters/JoinUtils.h`（让 full+other_condition 进入 row-flagged 路径）
  - `dbms/src/Interpreters/JoinPartition.cpp`（map 初始化、probe dispatch、adder not-found 行为）
  - `dbms/src/Interpreters/Join.cpp`（`doJoinBlockHash` 标记 used 时跳过空指针）
- 验收：
  - 场景：key 命中但 `other condition` 全失败时，右侧行仍能在 probe 后扫描阶段输出。

7. 第 7 步：核心改造 B - `handleOtherConditions` full 语义（通常在第 6 步一并落地）
- 目标：full 下也要有“外连接至少保留一行 + 右侧置 null”的正确行为。
- 修改：
  - `dbms/src/Interpreters/Join.cpp`（`handleOtherConditions` 分支）
- 验收：
  - full + other_condition 返回结果与“left/right outer 的对称组合语义”一致。

8. 第 8 步：probe 后扫描阶段联动（通常在第 6 步一并落地）
- 目标：full+other_condition 使用 row-flagged map 扫描未匹配右行。
- 修改：
  - `dbms/src/DataStreams/ScanHashMapAfterProbeBlockInputStream.cpp`
- 验收：
  - full+other_condition 能正确输出 build 侧未匹配行，不丢行、不重复。

9. 第 9 步：测试补齐（先 targeted）
- 目标：先保证 correctness，再扩矩阵。
- 修改建议：
  - `dbms/src/Flash/Coprocessor/tests/gtest_join_get_kind_and_build_index.cpp`
  - `dbms/src/Flash/tests/gtest_join_executor.cpp`
  - `dbms/src/Flash/tests/gtest_spill_join.cpp`（至少 1 组）
- 验收最小集：
  - full + key + 无 other_condition
  - full + key + 有 other_condition
  - full + key 命中但 other_condition 全失败（关键回归）
  - full + left/right conditions
  - full + null key

## 一句话风险提示

如果只做“JoinType 映射 + 输出 nullable”而不改 row-flagged 逻辑，`FULL OUTER JOIN ... ON eq_key AND other_condition` 会出现右侧漏行，属于结果错误而非性能问题，必须和协议打通一起完成。
