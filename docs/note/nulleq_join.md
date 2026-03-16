# TiFlash NullEQ Join Key（`<=>` / `tidbNullEQ`）设计文档

## 背景

TiFlash 当前 Hash Join 的默认等值语义是：

- `NULL` 不参与等值匹配
- build 侧含 `NULL` key 的行不会进入 hash map
- probe 侧含 `NULL` key 的行会被直接当作 not matched

这与 Null-safe equal（`<=>` / `tidbNullEQ`）要求的语义不同：

- `NULL <=> NULL` 为 `true`
- `NULL <=> non-NULL` 为 `false`
- `non-NULL <=> non-NULL` 与普通 `=` 一致

本设计文档讨论的是：在 TiFlash 已经支持 `FULL OUTER JOIN` 的前提下，如何为 hash join 增加 **join key 粒度的 NullEQ 语义**。

## 目标

1. TiFlash 的 Hash Join 在 **join key** 使用 NullEQ 语义时结果正确。
2. 支持 **key 粒度混合语义**：
   - 同一个 join 中允许部分 key 走 `=`
   - 允许部分 key 走 `<=>`
3. 未下发 NullEQ 标记时，保持现有行为不变。
4. 与已经支持的 `FULL OUTER JOIN` 语义兼容，不引入 `NULL <=> NULL` 相关的 outer join 错误结果。

## 非目标

1. 不把 `other_conditions` 中的 `<=>` 纳入本文新增的 join-key NullEQ 语义范围；若 planner 下发了这类表达式，按普通 other condition 处理。
2. MVP 不追求最优性能，允许为了正确性强制走 `serialized`。
3. 不扩大 NullAware join（`NOT IN` 家族）的语义覆盖范围；MVP 阶段建议 fail-fast。
4. 不在本轮实现 cartesian full join 与 NullEQ 的组合。

## 作用范围

本轮 scope 限定为：

- hash join
- `left_join_keys/right_join_keys` 非空
- NullEQ 只出现在 join key 上

不包含：

- cartesian join
- 仅通过 `other_conditions` 表达 NullEQ join 语义的计划形态
- planner 把 `<=>` 重写成其它表达式后再由 TiFlash 反推语义

## 输入契约

### tipb 协议

建议在 `tipb::Join` 中增加：

- `repeated bool is_null_eq = ...;`

语义如下：

- `is_null_eq[i] = false`：第 `i` 对 join key 使用普通 `=`
- `is_null_eq[i] = true`：第 `i` 对 join key 使用 `<=>`

长度约束：

- `is_null_eq_size == 0`：视为全 `false`，兼容旧版本
- 否则必须满足：
  - `is_null_eq_size == left_join_keys_size`
  - `is_null_eq_size == right_join_keys_size`

### Join key 表达形式

MVP 假设 join key 由 planner 下发为列引用：

- `left_join_keys[i]` 与 `right_join_keys[i]` 是一一对齐的 key pair
- TiFlash 如需在执行层插入 cast 做类型对齐，不改变 key 的顺序和数量
- `is_null_eq[i]` 始终按 key pair index 对齐，而不是按 build/probe 角色对齐

### 语义边界

NullEQ 语义只通过 `is_null_eq[]` 表达：

- 若 `<=>` 出现在 `other_conditions` 中，则按普通布尔表达式处理；本文不要求执行层从 `other_conditions` 里的 `<=>` 反推出“这是 join key NullEQ”
- 不让执行层从通用表达式里反推某个 key 是否是 NullEQ

### 与 NullAware join 的关系

`is_null_aware_semi_join` 与 NullEQ 是两套不同语义：

- NullAware join 关注 `NOT IN` 的三值逻辑
- NullEQ 关注 join key 的比较语义

MVP 建议：

- 若 `is_null_aware_semi_join=true` 且存在任意 `is_null_eq[i]=true`，直接 fail-fast

原因是这两条路径都对“NULL key 行怎么处理”有强假设，混用很容易产生 silent wrong result。

## 当前实现的关键假设

当前 Join 框架里，与 NullEQ 直接冲突的假设主要有四类。

### 1. key-NULL 会被提前过滤

当前 build/probe 都会把 nullable key 做两件事：

1. 把 `ColumnNullable` 替换成 nested column
2. 把 key 中的 `NULL` 行写入 `null_map`

对应路径：

- build：`Join::insertFromBlockInternal()`
- probe：`ProbeProcessInfo::prepareForHashProbe()`

这意味着：

- build 侧 `NULL` key 行默认不入 map
- probe 侧 `NULL` key 行默认不 probe map

这与 NullEQ 的 `NULL <=> NULL` 可以匹配直接冲突。

### 2. side-condition 与 key-NULL 共用一张 null_map

当前 `recordFilteredRows()` 会复用同一张 `null_map`，把 side-condition 过滤结果与 “key 是否为 NULL” 混到一起。

对 NullEQ 来说，问题不在于执行链路必须长期维护两张独立的 map，而在于：

- 普通 `=` key 的 `NULL` 过滤
- left/right side-condition 过滤

这两类来源在**生成过滤结果**时必须区分，因为：

- 对普通 `=` key，应把 key 的 `NULL` 写入最终过滤结果
- 对 NullEQ key，不应把 key 的 `NULL` 写入最终过滤结果
- side-condition 的过滤结果则始终需要写入最终过滤结果

因此更准确的做法是：

- 先按 key 粒度决定哪些 `NULL` 需要参与过滤
- 再与 side-condition 的过滤结果合并成一张统一的 `row_filter_map`

也就是说，最终可以只有一张“这一行是否跳过 insert/probe”的 map，但不能继续沿用当前这种“先无差别把所有 key-NULL 都写进 null_map，再复用它叠加 side-condition”的实现方式。

### 3. RowsNotInsertToMap / scan-after-probe 默认把 NULL key 当作天然 unmatched

对于 right/full/right semi/right anti/null-aware 这些需要保留 build 侧特殊行的 join kind，当前实现会把“未入 map 的 build 行”记进 `RowsNotInsertToMap`，之后在 scan-after-probe 阶段输出。

在普通 `=` 语义下这成立，因为 key-NULL 本来就不参与匹配。

但在 NullEQ 语义下：

- `NULL` key 行不一定是 unmatched
- 它可能应该入 map，并与 probe 侧 `NULL` key 行成功匹配

### 4. KeyGetter 默认不编码 nullable bitmap

当前 `keys128/keys256` 这类 fixed key hash method 默认是 `has_nullable_keys = false`。

这意味着即使不提前过滤 `NULL`，现有 packed key 路径也未必能正确把 nullness 编进 hash key。

## 与 FULL OUTER JOIN 的额外交互

NullEQ 本身不是 `FULL OUTER JOIN` 专属问题，`LEFT OUTER JOIN` 和 `RIGHT OUTER JOIN` 也会受影响。
但在 TiFlash 已经支持 `FULL OUTER JOIN` 之后，有几件事必须在设计里显式纳入，否则很容易出现双边都错的结果。

### 1. “NULL key 走天然 unmatched 路径”不是 full 特有问题，但 full 会把问题放大

这件事对不同 outer join 的影响不同：

- `LEFT OUTER JOIN`
  - probe 侧 `NULL` key 若仍直接走 `addNotFound()`，本该命中的 `NULL <=> NULL` 会被错误输出成左 unmatched
- `RIGHT OUTER JOIN`
  - build 侧 `NULL` key 若仍进 `RowsNotInsertToMap`，本该命中的行会在 scan-after-probe 阶段被错误输出成右 unmatched
- `FULL OUTER JOIN`
  - 上述两条路径会同时存在
  - 一组本该匹配的 `NULL <=> NULL` 行，可能被错误拆成：
    - 一条左 unmatched
    - 一条右 unmatched

所以这不是 full 独有问题，但 full 会把问题表现得最明显、也最复杂。

### 2. FULL + other condition 必须继续沿用“延后 setUsed”语义

当前 full 分支已经为 `full + other condition` 做了专门修正：

- key 命中时不能立刻 `setUsed()`
- 必须等 `other condition` 真正通过后，再标记 build 行为 used

否则 probe 后扫描阶段会漏输出本应作为 unmatched build 行的记录。

在 NullEQ 引入后，这个约束仍然成立，而且要覆盖 `NULL <=> NULL` 命中的情况：

- key 通过是因为 `NULL <=> NULL`
- other condition 失败
- 正确语义应该是：
  - 左侧保留一条右补 null 的 unmatched 行
  - 右侧 build 行仍然在后扫阶段输出为 unmatched

因此 NullEQ 不能绕开 full 分支当前的 row-flagged / delayed-used 设计。

### 3. RowsNotInsertToMap 在 full 下要重新定义语义

在支持 full 之后，`RowsNotInsertToMap` 不能再简单理解成“所有 NULL key 行 + 所有 build condition 失败的行”。

更准确的语义应该是：

- build side-condition 失败的行
- 普通 `=` key 因 key-NULL 被过滤的行

不应包含：

- NullEQ key 为 `NULL` 的行

因为这些行应该入 map，并可能成功匹配。

### 4. dispatch hash / spill / fine-grained shuffle 在 full 下更容易暴露错误

如果 build/probe 的 dispatch hash 没有把 nullness 编进 key：

- build 侧 `NULL` key 与 probe 侧 `NULL` key 可能落到不同 partition
- inner join 下通常表现为“不命中”
- full join 下则可能进一步演变成：
  - probe 侧输出一条 unmatched
  - build 侧后扫再输出一条 unmatched

所以 `full + NullEQ + spill/FGS` 应该是 MVP 测试矩阵里的必测项，而不是后续补充项。

### 5. full 的 schema nullable 规则不需要为 NullEQ 再单独扩展

这一点反而不用新增复杂度：

- full 输出 schema 两边本来就都应为 nullable
- other-condition 输入 schema 两边也已经按 full 语义处理成 nullable

NullEQ 改变的是 **匹配语义**，不是 full 输出 schema 的 nullable 规则。

## 设计选择

## 1. 总体原则

NullEQ 设计遵循两个核心原则：

1. 把“key 是否为 NULL”与“row 是否因 side-condition 被过滤”分离
2. 让 NullEQ 的 `NULL` 真正进入 key 比较，而不是继续被当成特殊 unmatched 行

由此得到两个概念：

- `row_filter_map`
  - 表示这一行不需要 insert/probe
  - 原因可以是 left/right condition 失败，也可以是普通 `=` key 的 `NULL`
- `key_null_map`
  - 只对普通 `=` key 有意义
  - NullEQ key 不应把 `NULL` 写进这张 map

这里保留这两个名字，主要是为了说明“过滤结果的来源”。

最终实现里，它们完全可以合并成一张统一的 `row_filter_map`：

- 普通 `=` key 的 `NULL` 可以进入这张 map
- left/right side-condition 的过滤结果也进入这张 map
- 但 NullEQ key 的 `NULL` 不能进入这张 map

换句话说，关键不是最终一定要维护两张独立的 map，而是生成最终过滤结果时，必须按 key 粒度决定哪些 `NULL` 应该被当作“跳过 insert/probe”的条件。

## 2. build/probe 都按 key 粒度区分 `=` 与 `<=>`

对于每个 key pair：

- 若 `is_null_eq[i] = false`
  - 延续现有 `=` 语义
  - key 中有 `NULL` 时，这一行不参与匹配
- 若 `is_null_eq[i] = true`
  - 保留 nullable key
  - `NULL` 可以参与 hash / probe / match

也就是说，NullEQ 不是“整条 join 全都变 null-safe”，而是按 key pair 生效。

## 3. build 路径设计

build 阶段的目标是：

- NullEQ key 为 `NULL` 的行可以入 map
- 普通 `=` key 为 `NULL` 的行仍不入 map
- side-condition 失败的行不入 map，但 outer join 语义所需的保底输出仍要保留

建议做法：

1. 从原始 key columns 出发，不再无条件对所有 key 调用 `extractNestedColumnsAndNullMap`
2. 遍历每个 key：
   - 对 `=` key：
     - 若是 nullable，则取 nested column
     - 并把该列 null map OR 进 `row_filter_map`
   - 对 `<=>` key：
     - 保留 `ColumnNullable`
     - 不把该列 null map 写进 `row_filter_map`
3. 再把 build side-condition 的过滤结果 OR 进 `row_filter_map`
4. 传给 `JoinPartition::insertBlockIntoMaps(..., row_filter_map, ...)`

这样 build 路径上的语义就变成：

- `row_filter_map[i] = 1`
  - 这一行不入 map
- `row_filter_map[i] = 0`
  - 这一行入 map

对于 full/right outer/right semi/right anti 这些会记录 build 特殊行的 join kind：

- 只有 side-condition 失败的行、或者普通 `=` key 的 `NULL` 行，才进入 `RowsNotInsertToMap`
- NullEQ key 的 `NULL` 行不应进入 `RowsNotInsertToMap`

## 4. probe 路径设计

probe 阶段的目标是：

- NullEQ key 为 `NULL` 的行可以真正 probe map
- 普通 `=` key 的 `NULL` 行仍然按“不匹配”处理
- left/full outer 语义下，probe unmatched 的保底输出仍然正确

建议做法与 build 对称：

1. 不再无条件对所有 key 做 `extractNestedColumnsAndNullMap`
2. 逐 key 处理：
   - `=` key 的 `NULL` 写入 `row_filter_map`
   - `<=>` key 保留 nullable，不写入 `row_filter_map`
3. 再把 probe side-condition 的过滤结果 OR 进 `row_filter_map`
4. probe 时：
   - `row_filter_map[i] = 1` 的行继续走历史 unmatched 路径
   - `row_filter_map[i] = 0` 的行真正进入 hash probe

这条规则对 outer join 的影响是：

- `LEFT OUTER JOIN` 不会再把 NullEQ 的 `NULL` probe 行过早打成 unmatched
- `FULL OUTER JOIN` 同理，但还要与后扫 build unmatched 语义一起对齐

## 5. Hash key 编码策略

### MVP 方案

当存在 **nullable 的 NullEQ key** 时，强制走：

- `JoinMapMethod::serialized`

原因很直接：

- `serialized` 能自然把 `ColumnNullable` 的 nullness 编进 key
- 可以先保证正确性，避免在 MVP 阶段就同时重写 packed key 路径

但这里有一个必须显式满足的前提：

- `serialized` 只是在“当前列对象长什么样”这个层面保留 nullness
- 它不会自动把 `Nullable(T)` 与 `T` 归一成同一种物理编码

当前 `ColumnNullable::serializeValueIntoArena()` 会先写入 null flag，再写 nested value。
因此对于同一个非空值：

- `Nullable(Int32)` 的序列化结果
- `Int32` 的序列化结果

并不相同。

这意味着：

- 若某个 NullEQ key pair 一侧是 nullable、另一侧是 non-nullable
- 即使 build/probe 两边都走 `serialized`
- 只要两边最终 key schema 仍分别是 `Nullable(T)` 与 `T`
- 相同的非空值也可能 hash / probe 不命中

因此 MVP 不能只做“nullable NullEQ 强制 serialized”，还必须保证：

- 对每个 `is_null_eq[i] = true` 的 key pair
- 只要任一侧最终需要保留 nullable 语义
- build/probe 两边就必须在 prepare key 阶段对齐到同一个物理 key schema
- 最直接的做法是统一到 `Nullable(common_type)`

这一步属于 `serialized` 正确性兜底的一部分，应该在后续 row_filter_map / RowsNotInsertToMap 语义改造之前完成。

### 后续优化方向

对 fixed-size key，后续可以参考 HashAgg 的 nullable packed keys：

- `keys128/keys256 + has_nullable_keys = true`

这样能把常见 nullable numeric / datetime 的 NullEQ join 拉回高性能路径。

## 6. JoinPartition / KeyGetter 语义

NullEQ 真正落地到 JoinPartition 时，关键不是“有没有 nullable 列”，而是：

- key getter 能不能把 nullness 编进 key

MVP 若强制 `serialized`，则这一步无需额外改动 key getter 类型分派，但仍要保证：

- build/probe 传进来的 key columns 保留了 NullEQ key 的 nullable 信息
- 对任意 NullEQ key pair，build/probe 两边最终参与编码的 key schema 一致
- 尤其是 mixed nullable / non-nullable 的场景，不能保留成 `Nullable(T)` 对 `T`

若后续要做 packed key 优化，则需要显式引入 nullable-aware 的 KeyGetter 分支。

## 7. FULL + other condition 语义

由于 full 分支已经有 row-flagged 逻辑，NullEQ 这里的要求不是新增一套 full 语义，而是确保 NullEQ key 命中也走已有正确链路：

1. 对 `full + other condition`：
   - 继续使用 row-flagged map
2. 对 key 命中但 other condition 失败的场景：
   - build 行的 used 标记必须延后到 other condition 通过后
3. 这个规则必须覆盖：
   - 普通值命中
   - `NULL <=> NULL` 命中

否则 full 下会出现漏右行或重复 unmatched 行。

## 8. RuntimeFilter

MVP 建议：

- 只要 join 含 NullEQ，且存在 nullable 的 NullEQ key，就禁用 runtime filter

原因：

- 当前 runtime filter / Set 路径仍然默认丢弃 `NULL` key
- 这与 NullEQ 的 “NULL 可以匹配” 冲突

更长期的方向可以是：

- Set 里额外维护 `has_null`
- 单列 NullEQ key 的 runtime filter 应用语义改成：
  - `isNull(x) ? has_null : (x IN set)`

但这不建议放进 MVP。

## 9. 备选方案：Planner 重写 `<=>`

另一条路线是让 TiDB planner 不显式下发 `is_null_eq[]`，而是把每个 `<=>` key 重写成：

1. `isNull(k)`
2. `ifNull(k, sentinel)`

这样 TiFlash 仍然走普通 `=` join。

这条路线的优点是执行层改动小，但缺点也很明显：

- 每个 `<=>` key 变成两个 key
- hash key 变宽
- planner/runtime filter/cast/collation 都会更绕
- key 级别语义变得不够直观

因此本设计默认选择：

- TiFlash 原生支持 key 粒度 NullEQ

## 测试建议

MVP 至少应覆盖：

1. `INNER JOIN`
   - `NULL <=> NULL` 命中
   - `NULL <=> 1` 不命中
2. `LEFT OUTER JOIN`
   - probe 侧 `NULL` key 不会被过早当作 unmatched
3. `RIGHT OUTER JOIN`
   - build 侧 `NULL` key 不会被错误塞进 `RowsNotInsertToMap`
4. `FULL OUTER JOIN`
   - `NULL <=> NULL` 命中时，不会被拆成两条 unmatched
   - `NULL <=> NULL` 命中但 `other condition` 失败时，左右 unmatched 都正确
5. `SEMI / ANTI`
   - `NULL <=> NULL` 参与存在性判断
6. 多列混合语义
   - `k1 <=> k1 AND k2 = k2`
   - `k1 <=> k1 AND k2 <=> k2`
7. side-condition 交互
   - left/right condition 与 NullEQ key 共存
8. spill / fine-grained shuffle
   - 特别是 `FULL OUTER JOIN + NullEQ`

## 代码热点

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

## 开发追踪 / Dev Note

这一节放在文档后半部分，用于后续按 checkpoint 推进时记录实现进度。设计结论以前面的章节为准。

### How to continue

继续开发前建议固定做三件事：

1. 先读本文件的设计部分
2. 再跑 `git status` / `git diff --stat`
3. 明确本次只推进哪个 checkpoint

建议在后续指令里直接写：

- “以 `docs/note/nulleq_join.md` 为准，从 CP2 开始继续”
- “先读设计文档，再读当前进度”

### Milestone 划分

#### Milestone 0：协议 / Plumbing

Done 标准：

- TiFlash 能解析 `is_null_eq[]`
- 能透传到 `DB::Join`
- 未下发该字段时行为零变化

#### Milestone 1：正确性 MVP

Done 标准：

- nullable NullEQ key 能正确 build / probe
- mixed nullable / non-nullable 的 NullEQ key pair 能正确对齐 key schema 并命中
- outer join / scan-after-probe 不把 NullEQ 的 `NULL` 行误判为 unmatched
- `FULL OUTER JOIN + other condition` 与 NullEQ 组合语义正确
- runtime filter 在该模式下被禁用

#### Milestone 2：测试矩阵

Done 标准：

- inner / left / right / full / semi / anti 的基础矩阵覆盖齐
- mixed key、side-condition、spill/FGS 覆盖齐

#### Milestone 3：性能优化

Done 标准：

- nullable fixed-size key 不再强制 serialized

#### Milestone 4：RuntimeFilter（可选）

Done 标准：

- 单列 NullEQ key 的 runtime filter 语义正确，或明确长期禁用

### Checkpoint 建议

- CP0：tipb 字段 + TiFlash 解析
- CP1：`DB::Join` 保存/打印 `is_null_eq`
- CP2.1：nullable NullEQ 强制 serialized + mixed-nullability key schema 对齐 + NullAware 互斥检查
- CP2.2：build/probe 的 row_filter_map 语义拆分
- CP2.3：`RowsNotInsertToMap` / scan-after-probe 调整
- CP2.4：`FULL OUTER JOIN + other condition` 与 NullEQ 联动自测
- CP2.5：MVP 禁用 runtime filter
- CP3：补测试
- CP4：packed keys 优化

### 当前进度

- 说明：以下勾选按当前 workspace 核对，用于记录本轮开发推进状态。
- [x] tipb: `Join.is_null_eq` 字段定义
- [x] TiFlash: `JoinInterpreterHelper::TiFlashJoin` 解析 `is_null_eq[]`
- [x] TiFlash: `DB::Join` 保存/打印 `is_null_eq`
- [x] TiFlash: nullable NullEQ 强制 serialized + mixed-nullability key schema 对齐 + NullAware 互斥 fail-fast
- [x] TiFlash: build/probe 的 row_filter_map 语义拆分
- [x] TiFlash: `RowsNotInsertToMap` / scan-after-probe 调整
- [x] TiFlash: `FULL OUTER JOIN + other condition` 与 NullEQ 联动验证
- [x] TiFlash: runtime filter 禁用
- [ ] TiFlash: gtest 覆盖
- [ ] TiFlash: packed keys 优化

### Open Questions

- TiDB / kvproto 何时同步 `is_null_eq[]`
- key 若未来允许表达式，`is_null_eq[i]` 如何稳定对齐
- string + collation 的性能回退是否可接受
- spill / FGS 场景下是否需要单独的 profile 或 debug 指标
- NullAware join 是否永远与 NullEQ 互斥，还是未来要定义组合语义
