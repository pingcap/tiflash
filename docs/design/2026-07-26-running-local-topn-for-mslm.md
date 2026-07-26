# TiFlash MSLM Running Local TopN 设计

- Author: TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## 目录

- [背景](#背景)
- [目标](#目标)
- [非目标](#非目标)
- [MVP 范围](#mvp-范围)
- [核心思路](#核心思路)
- [正确性](#正确性)
- [执行流程](#执行流程)
- [Running Local TopN 状态](#running-local-topn-状态)
- [Filter 坐标](#filter-坐标)
- [启用规则](#启用规则)
- [实现计划](#实现计划)
- [测试方案](#测试方案)
- [性能评估](#性能评估)
- [风险](#风险)
- [备选方案](#备选方案)
- [未解决问题](#未解决问题)

## 背景

TiFlash 现有 multi-stage late materialization (MSLM) 通过把 TableScan 上方的 residual Selection 下沉到 DeltaMerge scan 内部，先读取较少的 filter columns，再延迟读取最终输出所需的 final rest columns。

对于下面这类 SQL，MSLM 仍然存在进一步优化空间：

```sql
SELECT payload_cols
FROM t
WHERE selection_pred
ORDER BY order_key
LIMIT k;
```

如果 `payload_cols` 很宽，而 `ORDER BY ... LIMIT` 只需要少量 rows，理想情况下 TiFlash 可以先读取 selection columns 和 order-by columns，筛出 TopN 候选 rows，再读取这些候选 rows 的 payload columns。

更彻底的方案是让 storage 支持按 candidate row id 回读 rest columns。但当前 DeltaMerge storage 还没有提供这种能力。短期内可以在现有 MSLM 的 block lock-step 模型里做一个更保守的方案：在每个 MSLM stream 内维护 running local TopN，用它生成当前 block 的 candidate filter，只减少 final rest columns 的读取量。

## 目标

本设计希望在不引入 candidate row id 回读能力的前提下，减少 `TopN + Selection + TableScan` 场景中 final rest columns 的读取。

目标：

- 复用现有 MSLM 框架，不改变 DeltaMerge storage 的 block-level read/skip/readWithFilter 能力。
- 在 MSLM Stage 1 中读取 residual filter columns 和 order-by columns。
- 在每个 MSLM stream 内维护 running local TopN 状态。
- 使用 running local TopN 生成当前 block 的 candidate filter。
- 将 candidate filter 合并到 final rest columns 的读取 filter 中。
- 保留上层原有 TopN executor，由上层 TopN 负责最终全局结果。

## 非目标

第一版不解决以下问题：

- 不支持按 candidate row id 随机回读 rest columns。
- 不移除或替换上层 TopN executor。
- 不做跨 stream 的 storage 内全局 TopN merge。
- 不支持复杂 ORDER BY expression。
- 不支持 join、aggregation、window 或其他会改变 row cardinality 的算子下的 TopN 下推。
- 不对旧的非 pipeline DAG executor 路径做支持。
- 不追求和未优化路径在 ORDER BY ties 下选择完全相同的物理 rows。

## MVP 范围

第一版只支持下面范围：

- 只支持 `ORDER BY` plain columns。
- 只支持 constant `LIMIT` 和 constant `OFFSET`。
- 只支持单表 `Selection + TableScan + TopN` 或 `TableScan + TopN`。
- 不移除上层 TopN。
- 只在 MSLM 已经满足启用条件时启用。
- TopN filter 只用于减少 final rest columns 的读取。

其中本地 TopN 使用的 K 为：

```text
K = LIMIT + OFFSET
```

如果没有 OFFSET，则：

```text
K = LIMIT
```

## 核心思路

现有 MSLM 的逻辑可以简化为：

```text
Stage 0:
  读取 pushed filter columns
  执行 pushed filters
  得到 stage0_filter

Stage 1:
  根据 stage0_filter 读取 residual filter columns
  执行 residual filters
  得到 residual_filter

Final:
  根据 stage0_filter/residual_filter 读取 final rest columns
```

加入 running local TopN 后，逻辑变为：

```text
Stage 0:
  读取 pushed filter columns
  执行 pushed filters
  得到 stage0_filter

Stage 1:
  根据 stage0_filter 读取 residual filter columns + order-by columns
  执行 residual filters
  执行 running local TopN
  得到 topn_candidate_filter

Final:
  根据 stage0_filter && residual_filter && topn_candidate_filter
  读取 final rest columns
```

`topn_candidate_filter` 只用于减少 final rest columns 的读取。上层 TopN executor 仍然保留，因此 storage 内输出可以是全局 TopN 的 superset。

## 正确性

### Local TopK 覆盖 Global TopK

对于任意一个 stream，如果某一行 `r` 是全局 TopK 结果中的一行，那么 `r` 必然属于它所在 stream 的 local TopK。

证明：

如果 `r` 不属于所在 stream 的 local TopK，说明这个 stream 中至少有 K 行按照相同 comparator 排在 `r` 前面。这 K 行在全局也排在 `r` 前面，因此 `r` 不可能是全局 TopK，矛盾。

所以：

```text
global TopK subset union(each stream local TopK)
```

第一版保留上层全局 TopN executor，因此只要 storage 内输出包含全局 TopK 的 superset，最终结果就是正确的。

### Running Local TopN 的 Streaming Superset

MSLM stream 是 streaming 输出，不能撤回已经输出的 rows。Running local TopN 处理后续 blocks 时，可能会把之前已经输出的 candidate rows 挤出当前 local TopK。

这不会影响正确性，因为这些 rows 只是额外候选。上层 TopN 会再次排序并裁剪。

因此 running local TopN 输出的是：

```text
final local TopK 的 superset
```

而不是严格等于 final local TopK。

### 被跳过 rows 不会重新进入 TopK

当 local TopN heap 已满时，如果当前 row 按 comparator 不优于 heap worst row，则该 row 可以被跳过。原因是后续扫描只会增加竞争者，不会让这个 row 的排序位置变得更靠前。

对于和 heap worst row 相等的 ties，第一版采用保守策略：相等 rows 作为 candidate 输出，但不一定放入 bounded heap。这会降低 ties 较多场景下的剪枝效果，但可以避免因为 ties 选择不同物理 rows 引起不必要的行为差异。

## 执行流程

### 计划识别

TiFlash 在构建 TableScan pipeline 时识别下面模式：

```text
TopN
  |
Selection (optional)
  |
TableScan
```

识别成功后，生成一个 MSLM TopN 描述：

```text
struct MSLMTopNDescription
{
    SortDescription sort_description;
    UInt64 limit;
    UInt64 offset;
    UInt64 topk; // limit + offset
    ColumnDefines order_by_columns;
};
```

第一版 `order_by_columns` 只允许 plain columns，不构造复杂 expression actions。

### Stage 1 列集合

开启 TopN-enhanced MSLM 后，Stage 1 需要读取：

```text
stage1_columns = residual_filter_columns union order_by_columns
```

如果某个 order-by column 已经属于 residual filter columns，则去重后只读一次。

如果 order-by column 也属于最终输出 columns，它仍然可以作为 Stage 1 column 提前读取，final rest columns 中需要排除该列，避免重复读取。

### Running Local TopN 处理

每个 `MultiStageLateMaterializationBlockInputStream` 持有一个 local TopN heap。

处理一个 block 时：

1. Stage 0 读取 pushed filter columns，得到 `stage0_filter`。
2. Stage 1 根据 `stage0_filter` 读取 `stage1_columns`。
3. 执行 residual filters，得到 `residual_filter`。
4. 对 residual passed rows 计算 order key。
5. 使用 order key 更新 local TopN heap。
6. 为当前 block 生成 `topn_candidate_filter`。
7. 使用 combined filter 读取 final rest columns。
8. hstack Stage 1 columns 和 final rest columns。
9. 上层 TopN executor 继续执行全局 TopN。

伪代码：

```text
for each stage0 block:
    stage0_filter = execute_stage0_filter(block)
    if stage0_filter passes no rows:
        skip stage1 stream
        skip final_rest stream
        continue

    stage1_block = read stage1 columns with stage0_filter
    residual_filter = execute_residual_filter(stage1_block)
    if residual_filter passes no rows:
        skip final_rest stream
        continue

    topn_candidate_filter = running_topn.update(stage1_block, residual_filter)
    if topn_candidate_filter passes no rows:
        skip final_rest stream
        continue

    combined_filter = compose(stage0_filter, residual_filter, topn_candidate_filter)
    final_rest_block = read final_rest columns with combined_filter
    stage1_block = filter stage1_block by residual_filter && topn_candidate_filter
    return hstack(stage1_block, final_rest_block)
```

### Current Block 内 Eviction

如果当前 block 的某个 row 先进入 heap，但随后在同一个 block 内被更优 row 挤出，第一版可以把它从当前 block 的 candidate filter 中清除。

如果被挤出的 row 来自之前已经输出的 block，则不能撤回。该 row 会作为额外候选留给上层 TopN 过滤。

Heap entry 可以记录：

```text
struct HeapEntry
{
    SortKey key;
    UInt64 stream_sequence;
    UInt64 block_sequence;
    UInt32 row_index_in_stage1_block;
    bool current_block_output_candidate;
};
```

`block_sequence` 用于判断被 eviction 的 entry 是否属于当前 block。

## Running Local TopN 状态

每个 MSLM stream 维护独立状态：

```text
class RunningLocalTopN
{
public:
    Filter update(const Block & stage1_block, const FilterPtr & residual_filter);

private:
    UInt64 topk;
    SortDescription sort_description;
    PriorityQueue heap; // heap top is current worst candidate
};
```

Heap 大小最多为 `topk`。如果 `topk` 超过启发式阈值，则禁用该优化。

比较器必须和上层 TopN 使用的 comparator 保持一致。第一版只支持 plain order-by columns，减少 comparator 语义不一致风险。

## Filter 坐标

MSLM 中存在多套 filter 坐标：

- `stage0_filter`: 原始 Stage 0 block rows 坐标。
- `residual_filter`: Stage 1 block rows 坐标，即 Stage 0 passed rows 坐标。
- `topn_candidate_filter`: Stage 1 block rows 坐标。
- `combined_filter`: 原始 Stage 0 block rows 坐标，用于读取 final rest columns。

组合规则：

```text
stage1_final_filter = residual_filter && topn_candidate_filter
combined_filter = compose(stage0_filter, stage1_final_filter)
```

当 `stage0_filter` 为 nullptr，即 Stage 0 全通过时，`combined_filter` 可以直接基于 `stage1_final_filter` 构造。

当 `stage1_final_filter` 全通过时，可以退化为现有 MSLM 路径。

当 `stage1_final_filter` 全过滤时，需要 skip final rest stream。

## 启用规则

第一版使用保守 rule-based 判断。

必须满足：

- `dt_enable_multi_stage_late_materialization` 为 true。
- 当前查询已经满足 MSLM 启用条件。
- 查询包含 TopN，且 TopN 位于单表 Selection/TableScan 之上。
- TopN 上方仍保留全局 TopN executor。
- TopN 的 `LIMIT` 和 `OFFSET` 是常量。
- `topk = limit + offset` 未超过阈值。
- TopN 之前的 Selection predicates 全部在 MSLM Stage 0 或 Stage 1 内执行。
- ORDER BY 只包含 plain columns。
- ORDER BY columns 可加入 Stage 1 column set。
- ORDER BY columns 数量不超过阈值。
- final rest columns 数量满足 MSLM 的宽列启发式要求。

建议第一版阈值：

```text
topk <= 4096
order_by_column_count <= 4
```

实际阈值可以根据 benchmark 调整。

禁用场景：

- ORDER BY expression 不是 plain column。
- ORDER BY 依赖 generated column。
- TopN 包含非常大的 offset。
- TopN 之前仍有无法进入 MSLM 的 Selection。
- 查询要求 keep order 且和 MSLM 现有限制冲突。
- TopN 位于 join、aggregation、window 等算子之上。

## 实现计划

### Step 1: 文档和测试用例设计

- 明确 MVP 范围。
- 增加基于 DAG request 的单测设计。
- 明确 ties、offset、NULL ordering、collation 的测试覆盖。

### Step 2: Plan 识别和元数据传递

- 在 TableScan pipeline 构建阶段识别 `TopN -> Selection -> TableScan` 模式。
- 构造 `MSLMTopNDescription`。
- 将 order-by columns 合并到 MSLM Stage 1 column set。
- 保留上层 TopN executor，不修改最终 DAG 语义。

### Step 3: RunningLocalTopN 状态

- 实现 per-stream bounded TopK heap。
- 支持 plain column sort key 提取。
- 支持 ASC/DESC、NULL ordering 和 TiFlash 现有 comparator 语义。
- 生成当前 block 的 `topn_candidate_filter`。

### Step 4: 接入 MSLM final rest 读取

- 在 residual filter 之后执行 running local TopN。
- 将 `residual_filter` 和 `topn_candidate_filter` 合并。
- 使用 combined filter 读取 final rest columns。
- 当 TopN filter 无剪枝效果时，允许退化到现有路径。

### Step 5: Runtime Stats 和可观测性

- 保留现有 TableScan/Selection actRows 语义。
- 可选增加 debug log，输出 TopN candidate rows、heap size、filtered rows。
- 可选增加 scan context counters，用于统计 TopN filter 节省的 final rest rows。

## 测试方案

### 功能测试

- `ORDER BY c ASC LIMIT k`。
- `ORDER BY c DESC LIMIT k`。
- `ORDER BY c ASC LIMIT offset, count`。
- `WHERE + ORDER BY + LIMIT`，其中 WHERE 同时包含 Stage 0 pushed filter 和 Stage 1 residual filter。
- ORDER BY column 不在最终 projection 中。
- ORDER BY column 同时属于 residual filter columns。
- 多 block 数据，确保 running local TopN 可以跨 block 收紧 threshold。
- 多 stream 并发，确保上层 TopN 保证最终结果。

### 正确性测试

- 和未启用优化的执行结果比较。
- LIMIT/OFFSET 场景比较。
- NULL ordering 场景比较。
- ties 场景比较，验证结果满足 SQL order semantics。
- Stage 0 全过滤。
- residual filter 全过滤。
- TopN candidate filter 全过滤。
- TopN candidate filter 全通过。

### 回归测试

- MSLM 原有测试必须全部通过。
- TopN 上方的 projection 仍然正确。
- EXPLAIN ANALYZE actRows 不出现明显异常。
- 禁用条件下必须走原路径。

## 性能评估

重点 benchmark 场景：

- 宽表，payload columns 多且冷数据。
- `LIMIT` 小，`topk` 远小于 block rows。
- `ORDER BY` columns 少且窄。
- 数据分布有利于 running local TopN threshold 快速收紧。

需要对比：

- MSLM disabled。
- MSLM enabled but without running local TopN。
- MSLM enabled with running local TopN。

关键指标：

- Query latency。
- final rest columns read rows。
- DMFile read bytes。
- CPU time。
- memory usage。
- candidate rows output to upper TopN。

## 风险

### Comparator 语义不一致

如果 storage 内 running local TopN 的 comparator 和上层 TopN 不一致，可能错误过滤掉本应保留的 rows。

缓解：

- MVP 只支持 plain columns。
- 复用现有排序比较逻辑。
- 对 ASC/DESC、NULL、collation 做单测。

### Selection 顺序错误

如果 TopN filter 在某些 Selection predicates 执行之前生效，可能过滤掉最终应该进入 TopN 的 rows。

缓解：

- 只在 TopN 之前的 Selection 已全部进入 Stage 0/Stage 1 时启用。
- 无法确认时禁用。

### 收益不足或性能回退

如果 `topk` 大、order-by columns 宽、ties 很多，running local TopN 可能减少不了 final rest 读取，还增加 Stage 1 读取和 heap 维护成本。

缓解：

- 使用保守启发式启用。
- `topk` 和 order-by column count 设置上限。
- 对 ties 多的场景 benchmark。

### Streaming Superset 过大

Running local TopN 不能撤回之前已经输出的 rows。极端数据分布下，每个 block 都可能输出一批候选 rows。

缓解：

- 上层 TopN 保留，保证正确性。
- 通过 stats 观察 candidate rows 数量。
- 后续考虑更强的 candidate row id materialization。

## 备选方案

### Block-local TopN

每个 block 独立做 TopK，不维护跨 block state。

优点：

- 实现更简单。
- 正确性证明直接。
- 不需要处理 previous block candidate eviction。

缺点：

- 最多输出 `K * block_count` 个候选 rows。
- 无法利用前面 blocks 已经得到的 threshold 剪枝后续 blocks。

Running local TopN 是 block-local TopN 的增强版，复杂度增加有限，但剪枝能力更好。

### Candidate Row ID 回读

先读取 selection/order-by columns，得到全局或局部 TopN row ids，再按 row ids 回读 payload columns。

优点：

- 最接近理想 late materialization。
- 可以只 materialize 很少 rows。

缺点：

- 需要 DeltaMerge storage 支持按 candidate row id 高效回读 columns。
- 当前不具备该能力。
- 设计和实现复杂度明显更高。

本设计选择 running local TopN 作为短期方案。

## 未解决问题

- `topk` 阈值应该如何设置。
- ties 场景是否需要为了结果稳定性保留所有 equal-worst rows。
- 是否需要支持 ORDER BY expression，以及 expression action 如何复用。
- 是否需要在 EXPLAIN ANALYZE 中暴露 TopN-enhanced MSLM 的 candidate rows。
- 是否需要在 cost model 中引入列宽和 TopN selectivity。
