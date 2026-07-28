# TiFlash 多阶段 Late Materialization 设计

- Author: TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## 目录

- [背景](#背景)
- [目标](#目标)
- [非目标](#非目标)
- [现有实现](#现有实现)
- [总体设计](#总体设计)
- [启用规则](#启用规则)
- [执行流程](#执行流程)
- [自适应策略](#自适应策略)
- [列复用和 MVCC 列处理](#列复用和-mvcc-列处理)
- [Runtime Stats](#runtime-stats)
- [实现阶段](#实现阶段)
- [测试方案](#测试方案)
- [风险](#风险)
- [备选方案](#备选方案)
- [未解决问题](#未解决问题)

## 背景

一些 TiFlash 查询会同时满足下面几个特征：

- 查询条件具有一定过滤率。
- `SELECT` 列非常宽，可能包含二十多个甚至更多列。
- 查询访问冷数据，读取列数据时需要大量磁盘 IO。
- TiDB 已经把一部分 filter 下推到 TiFlash TableScan 的 late materialization filter 中，但仍有一部分 residual Selection 保留在 TableScan 上方。

典型 SQL 如下：

```sql
SELECT
  /*+ ignore_plan_cache() read_from_storage(tiflash[t]) */
  t.c1,
  t.c2,
  t.c3,
  t.c4,
  t.c5,
  t.c6,
  t.c7,
  t.c8,
  t.c9,
  t.c10,
  t.c11,
  t.c12,
  t.c13,
  t.c14,
  t.c15,
  t.c16,
  t.c17,
  t.c18,
  t.c19,
  t.c20,
  t.c21,
  t.c22,
  t.c23,
  t.c24,
  t.c25,
  t.c26,
  t.c27,
  t.c28
FROM t_wide t
WHERE
  t.c1 IN (?)
  AND t.c2 >= ?
  AND t.c2 <= ?
  AND t.c3 = ?
  AND t.c4 = ?
ORDER BY t.c5 DESC, t.c29 DESC
LIMIT ?, ?;
```

现有执行计划中，TableScan 只执行 planner 选择出的 pushed down filter。剩余 filter 会在 TableScan 之上的 Selection 中执行。由于 TiFlash 当前 late materialization 只在 pushed down filter 范围内生效，TableScan 在 residual Selection 执行之前就已经读取了所有输出列。对于宽表冷数据，这会导致显著读放大。

这个设计的核心目标是把 late materialization 的收益从 TableScan 自身的 pushed down filter 扩展到 TableScan 上方的 residual Selection，使 TiFlash 可以在读取最终宽列之前先执行 residual filters。

## 目标

本设计希望解决的问题：

- 在 pipeline 执行模式下，将 TableScan 上方的 residual Selection 融入 DeltaMerge scan 的 late materialization 流程。
- 在 residual filters 过滤率足够高时，避免提前读取大量 final rest columns。
- 对弱过滤率 residual filters 使用自适应降级，避免因为多阶段读造成明显性能回退。
- 保持表达式语义与现有 TiFlash Selection 执行一致，包括类型转换、timezone、collation、NULL 语义、常量表达式和临时 filter column 清理。
- 保证 TiDB `EXPLAIN ANALYZE` 仍然能看到 Selection executor 的 runtime stats，并且 TableScan/Selection 的 `actRows` 至少能分别表示 Stage 0 和 Stage 1 输出行数。
- 第一版只考虑 pipeline 执行路径，不考虑旧的 BlockInputStream DAG executor 路径。

## 非目标

第一版不解决以下问题：

- 不做 TopN pushdown 到 storage。
- 不做跨 storage 线程的全局 TopN 合并。
- 不引入 row reference 或二次 point lookup 读取最终列。
- 不做基于真实列宽的 cost model。
- 不在 TiDB planner 中重写复杂的 late materialization 代价模型。
- 不支持 generated column 参与该优化。第一版遇到 generated column 直接禁用。
- 不追求 TableScan 和 Selection runtime stats 的耗时、bytes、loops 严格拆分；第一版只拆分 `actRows`。
- 不考虑旧的 `DAGPipeline` / BlockInputStream executor 路径。

## 现有实现

TiFlash 当前 late materialization 主要围绕 `PushDownFilter` 和 `LateMaterializationBlockInputStream` 实现。

关键路径：

- `DAGStorageInterpreter::executeImpl(PipelineExecGroupBuilder & group_builder)` 构建 pipeline TableScan。
- `DeltaMergeStore::read(...)` 构建 DeltaMerge scan source。
- `DMSegmentThreadSourceOp` 和 `UnorderedSourceOp` 是 pipeline scan source。
- `Segment::getLateMaterializationStream(...)` 构造 late materialization stream。
- `PushDownFilter::build(...)` 基于 pushed down filters 构造 filter expression actions。
- `FilterTransformAction` 可以返回 filter bitmap。
- `DMFileReader::readWithFilter(...)` 依赖 filter bitmap 读取 rest columns。

当前 late materialization 的逻辑可以简化为：

1. 读取 pushed down filter 需要的列。
2. 执行 pushed down filter，得到 `base_filter`。
3. 根据 `base_filter` 读取所有剩余列。
4. 拼接 filter columns 和 rest columns，返回完整 block。
5. TableScan 上方的 residual Selection 再执行剩余 filters。

问题在第 3 步。对于 residual Selection 过滤率很高的查询，第 3 步会提前读取大量最终会被过滤掉的宽列。

## 总体设计

新增一个多阶段 late materialization 模式。逻辑上分为三个阶段：

- Stage 0: 现有 pushed down filter late materialization。
- Stage 1: residual Selection filter late materialization。
- Final stage: 读取最终输出所需的 rest columns。

术语：

- `stage0_filter`: TiDB planner 已经下推到 TableScan 的 filter 结果，对应当前 late materialization 的 filter。
- `residual_filters`: TableScan 上方 Selection 中尚未进入 TableScan pushed down filter 的 conditions。
- `stage1_filter_columns`: 执行 residual_filters 需要读取的列。
- `final_rest_columns`: 最终输出需要但不属于 stage1_filter_columns 的列。
- `residual_filter`: residual_filters 在 stage0 输出行上的过滤结果。
- `combined_filter`: 把 `stage0_filter` 和 `residual_filter` 合并后得到的原始 block 坐标上的 filter，用于读取 final_rest_columns。

新的执行流程：

```text
原始 rows
  |
  | 读取 stage0 filter columns
  v
执行 pushed down filters
  |
  | 得到 stage0_filter
  v
按 stage0_filter 读取 stage1_filter_columns
  |
  | 执行 residual_filters
  v
得到 residual_filter
  |
  | 根据 residual_filter 的过滤率选择模式
  | 
  +-- LateMode:
  |     生成 combined_filter
  |     按 combined_filter 读取 final_rest_columns
  |     filter stage1 block
  |     hstack stage1 block 和 final rest block
  |
  +-- DirectMode:
        按 stage0_filter 读取 final_rest_columns
        hstack stage1 block 和 final rest block
        在内存中过 residual_filter
```

DirectMode 不需要新增 `full_stream`。它本质上还是两次读取：

- 第一次读取 residual filter columns。
- 第二次读取 final rest columns。

区别是第二次读取只使用 `stage0_filter`，不使用 `combined_filter`。这样可以避免 weak residual filters 下使用 `readWithFilter` 造成额外随机读或 pack 级别读放大。

## 启用规则

第一版使用保守的 rule-based 判断，不依赖列宽信息。

启用条件：

- TiFlash setting `dt_enable_multi_stage_late_materialization` 为 true。
- 当前 query 使用 pipeline 执行模式。
- TableScan 已存在 pushed down filters，即可以形成 Stage 0。
- TableScan 上方存在 residual Selection，即 `filter_conditions.hasValue()`。
- 不要求 `force_push_down_all_filters_to_scan`。
- 查询不涉及 generated columns。可以先用最保守规则：只要 `generated_column_infos` 非空就禁用。
- `keep_order` 为 false。需要保持 storage order 的查询先不启用。
- residual filters 能由现有 `DAGExpressionAnalyzer` / `PushDownFilter` 表达式路径构造。
- `stage1_filter_col_cnt <= 10`。
- `final_rest_col_cnt >= 12`。
- `final_rest_col_cnt >= 3 * stage1_filter_col_cnt`。

列数统计规则：

- 内部 MVCC visibility 需要的 handle/version/tag 不计入启发式列数。
- 如果 query 显式读取或过滤 handle/version 等列，则这些列按普通列计入。
- `stage1_filter_col_cnt` 按 residual_filters 需要读取的真实列去重后计算。
- `final_rest_col_cnt` 按最终输出 schema 中不属于 stage1_filter_columns 的列计算。

不增加 residual filters simple gate。原因是 residual filters 原本也需要在 TiFlash 中执行；把它放到 Stage 1 执行不会引入第二次表达式计算。只要表达式可以走现有 TiFlash Selection 表达式构建和执行路径，就可以保持语义一致。

## 执行流程

### Stage 0: 现有 pushed down filter

Stage 0 尽量复用现有 late materialization 实现。

输入：

- 原始 segment rows。
- TiDB TableScan 中的 `pushed_down_filter_conditions`。
- Stage 0 filter columns。

输出：

- `stage0_filter`，坐标是原始 block rows。

Stage 0 仍然负责：

- MVCC visibility bitmap。
- pack / RS index / local index 过滤。
- 当前 pushed down filter 的表达式执行。
- 为后续 stage 提供原始 block 坐标上的 filter。

### Stage 1: residual filters

Stage 1 使用 `stage0_filter` 读取 residual filters 需要的列。

输入：

- `stage0_filter`。
- `stage1_filter_columns`。
- `residual_filters`。

输出：

- `stage1_block`，包含 stage1_filter_columns，行数等于 Stage 0 过滤后的行数。
- `residual_filter`，坐标是 `stage1_block` 的 rows。

Stage 1 表达式构建需要复用现有路径：

- 使用 `DAGExpressionAnalyzer` 构建 filter expression。
- 复用现有 Selection 的 cast、timezone、collation、NULL 语义。
- 复用现有 filter tmp column 生成和清理逻辑。
- 常量 filter 使用现有表达式执行结果处理。

Stage 1 不复用 Stage 0 已经读出的列。即使 residual filters 需要 `c1` 这类 Stage 0 已经读过的列，也允许在 Stage 1 再读一遍。这样可以避免引入 `accumulated_block`、`stage_input.setStartOffset` 等跨 stage block 复用状态。

如果 Stage 1 读取的列也是最终输出列，则这些列可以直接保留在 `stage1_block` 中，Final stage 不再重复读取这些列。

### Final stage: 读取最终 rest columns

Final stage 负责读取最终输出需要的剩余列。

输入：

- `stage0_filter`。
- `residual_filter`。
- `final_rest_columns`。
- 当前自适应模式。

LateMode：

1. 将 `residual_filter` expand 回原始 block 坐标。
2. 与 `stage0_filter` 做 AND，得到 `combined_filter`。
3. 使用 `combined_filter` 读取 `final_rest_columns`。
4. 使用 `residual_filter` 过滤 `stage1_block`。
5. 使用现有 `hstackBlocks` 机制拼接 filtered `stage1_block` 和 final rest block。
6. 按最终 schema 投影输出。

DirectMode：

1. 使用 `stage0_filter` 读取 `final_rest_columns`。
2. 使用现有 `hstackBlocks` 机制拼接 `stage1_block` 和 final rest block。
3. 对拼接后的完整 block 应用 `residual_filter`。
4. 按最终 schema 投影输出。

LateMode 下需要一个新的 filter compose 工具函数：

```text
compose(stage0_filter, residual_filter):
  residual_pos = 0
  for i in [0, stage0_filter.size()):
    if stage0_filter[i] == 0:
      combined_filter[i] = 0
    else:
      combined_filter[i] = residual_filter[residual_pos]
      residual_pos += 1
  assert residual_pos == residual_filter.size()
```

这个函数不需要保存跨 block 的 offset。它只处理当前 block 内 `stage0_filter` 和 `residual_filter` 的坐标转换。

## 自适应策略

只使用静态 rule 会有回退风险。比如 residual filters 实际只能过滤约 20% rows 时，如果强行 LateMode，会多一次 stage1 filter column 读取和 hstack，收益不足。

因此 Stage 1 需要 runtime adaptive。

### 核心指标

自适应只看 residual filters 的过滤率，而不是 Stage 0 的过滤率。

```text
residual_filtered_ratio =
  (stage0_passed_rows - residual_passed_rows) / stage0_passed_rows
```

只有当 `residual_filtered_ratio > 0.5` 时，才认为 residual filters 足够强，值得进入 LateMode。

这个阈值偏保守，因为 DeltaMerge 读取存在 pack/block 级别读放大。即使最终只读取少量行，也可能至少读一个 pack，例如 8192 rows。因此 residual filters 需要过滤超过一半 rows，才更可能抵消多阶段读取的额外成本。

### 模式

每个 scan source 或每个 segment stream 独立维护自适应状态，不需要跨线程全局协调。

状态：

- `Sampling`: 采样阶段，收集 residual filters 的实际过滤率。
- `LateMode`: residual filters 足够强，final rest columns 使用 `combined_filter` 读取。
- `DirectMode`: residual filters 不够强，final rest columns 使用 `stage0_filter` 读取，然后内存中过 residual filter。

采样策略：

- 前几个 blocks 且前若干 Stage 0 passed rows 进入 Sampling。
- Sampling 期间仍然逐 block 输出正确结果。
- 每个 sample block 先读取 `stage1_filter_columns` 并计算 `residual_filter`。
- 对 sample block 可以按 block-local 过滤率选择 LateMode 或 DirectMode 输出。
- 同时达到 block 数和 Stage 0 passed rows 阈值后，根据累计 `residual_filtered_ratio` 固定后续模式。
- 如果 Stage 0 对每个 block 的过滤结果很稀疏，例如每个 block 只剩 1 行，不能只因为 block 数达到阈值就固定模式，否则 4 行样本会导致过滤率估计失真。

建议初始参数：

- `min_sample_blocks = 4`。
- `min_sample_rows = 16384`。
- `late_mode_filter_ratio_threshold = 0.5`。

这些参数可以先作为常量实现，后续再考虑暴露成 settings。

### 与现有 per-block fallback 的关系

现有 late materialization 已经有 per-block 判断：如果 filter 掉的行数太少，可能直接读 rest columns 再内存过滤，而不是调用 `readWithFilter`。

多阶段 LM 可以保留类似策略：

- Runtime adaptive 决定 Stage 1 的长期模式。
- LateMode 内部仍然允许 per-block fallback，避免某个 block 上 `combined_filter` 过滤太弱时触发不划算的 filtered read。

## 列复用和 MVCC 列处理

### Stage 0 列不跨 stage 复用

第一版不复用 Stage 0 已经读取过的业务列。

如果 Stage 0 和 Stage 1 都需要 `c1`：

- Stage 0 读取一次 `c1`，用于 pushed down filter。
- Stage 1 可以再读取一次 `c1`，用于 residual filters 或最终输出。

这个重复读取可以接受：

- Stage 0 读过的列通常已经变热。
- 避免跨 stage 保留 block、offset、row mapping。
- 实现复杂度低，正确性风险小。

### Stage 1 列可以给最终输出复用

Stage 1 读取的列如果属于最终输出 schema，则可以直接进入最终输出 block。

Final stage 的 `final_rest_columns` 应该排除 `stage1_filter_columns` 中已经可复用的输出列。

这样可以复用现有 `hstackBlocks` 拼 block 机制：

```text
output_block = hstackBlocks(filtered_stage1_block, final_rest_block)
project(output_block, final_schema)
```

不需要引入 `accumulated_block`。

### MVCC 列

内部 MVCC visibility 所需的 handle/version/tag 由 DeltaMerge 读取层使用，用于构造 MVCC bitmap。对于用户没有显式要求的 MVCC 内部列：

- 不计入启用规则中的列数。
- 不作为 Stage 1 filter columns 或 final rest columns。
- 不需要在多阶段 LM 中重复读取。

MVCC 对齐依赖 row id、segment row 坐标和 bitmap。后续业务列读取可以使用相同的 segment block 坐标和 bitmap 对齐，不需要再次读取 MVCC 三列并重新做 stable/delta merge。

如果 query 显式读取或过滤 handle/version/tag 相关列，则把这些列当作普通列处理：

- residual filters 需要它，就进入 `stage1_filter_columns`。
- 最终输出需要它，就进入 Stage 1 输出或 `final_rest_columns`。

## Runtime Stats

第一版只考虑 pipeline。

当前 pipeline 的 runtime stats 通过 `DAGContext::addOperatorProfileInfos` 注册：

- TableScan executor id 注册当前 scan pipeline 的 `OperatorProfileInfoPtr`。
- 普通 pushed down Selection 会 append `FilterTransformOp` 和 `ExpressionTransformOp`，然后把当前 profile infos 注册到 Selection executor id。

多阶段 LM 消费 residual Selection 后，不再 append 原来的 Selection transform。为了让 TiDB `EXPLAIN ANALYZE` 中仍然有 Selection execution summary，可以继续复用当前 scan pipeline 的 profile infos：

```cpp
dag_context.addOperatorProfileInfos(
    table_scan.getTableScanExecutorID(),
    group_builder.getCurProfileInfos(),
    /*is_append=*/true);

if (filter_conditions.hasValue() && multi_stage_lm_consumed_filter_conditions)
{
    dag_context.addOperatorProfileInfos(
        filter_conditions.executor_id,
        group_builder.getCurProfileInfos(),
        /*is_append=*/true);
}
```

不过，如果完全共享同一组 `OperatorProfileInfo`，TableScan 和 Selection 的 `actRows` 也会相同，无法看出 Stage 0 和 Stage 1 的行数差异。因此第一版增加一个很轻量的 rows override：

- `MultiStageLateMaterializationRuntimeStats::stage0_output_rows` 记录 Stage 0 pushed filter 后的输出行数。
- `MultiStageLateMaterializationRuntimeStats::stage1_output_rows` 记录 residual filters 后的输出行数。
- `DAGContext` 维护 `executor_id -> rows_override`。
- `ExecutorStatistics::collectRuntimeDetail()` 聚合原始 profile 后，如果存在 rows override，只覆盖 `base.rows`。

这样 TiDB 侧不需要新增协议字段，仍然使用 `ExecutorExecutionSummary.num_produced_rows` 展示 `actRows`：

- TableScan `actRows` 显示 Stage 0 输出行数。
- Selection `actRows` 显示 Stage 1 输出行数。
- execution time、bytes、allocated bytes、loops 等仍然来自同一组 scan pipeline profile，不承诺严格拆分。

这个方案的取舍：

- Selection 不会缺 runtime stats。
- 不需要新增 synthetic operator。
- 不需要在 storage reader 中手动维护独立完整 `OperatorProfileInfo`。
- 和 remote/null pipeline 分支中复用当前 profile infos 的做法一致。

## 实现阶段

### Stage A: 元数据和启用开关

目标：让 pipeline scan 能识别一次 residual Selection 是否会被 storage 内部消费。

改动点：

- 在 TiFlash pipeline TableScan 构建路径中识别 candidate：
  - `dt_enable_multi_stage_late_materialization` 为 true。
  - `filter_conditions.hasValue()`。
  - TableScan 已经有 pushed down filters。
  - `generated_column_infos` 为空。
  - `read_opts.keep_order == false`。
  - 列数满足启用规则。
- 增加 TiFlash setting `dt_enable_multi_stage_late_materialization`，并增加一个内部 flag，例如 `multi_stage_lm_enabled`。
- 将 residual filter conditions 传入 DeltaMerge read 层。
- 当 `multi_stage_lm_enabled` 为 true 时，跳过 `executePushedDownFilter(...)`。
- 同时把当前 `group_builder.getCurProfileInfos()` 注册给 `filter_conditions.executor_id`。

这个阶段先不改核心读取逻辑，可以先打通参数、日志、profile 注册和禁用条件。

建议增加日志字段：

- 是否启用 multi-stage LM。
- `stage1_filter_col_cnt`。
- `final_rest_col_cnt`。
- 禁用原因。

### Stage B: Stage 1 filter expression 构建

目标：复用现有 Selection 表达式语义构造 residual filter stage。

改动点：

- 基于 `filter_conditions.conditions` 构造 Stage 1 `PushDownFilter` 或等价结构。
- 复用 `DAGExpressionAnalyzer::buildPushDownFilter` 的语义。
- 生成：
  - `before_where`。
  - `filter_column_name`。
  - `project_after_where`。
  - `stage1_filter_columns`。
- 处理常量 filter：
  - 常量 true：Stage 1 可以退化为 DirectMode 或跳过 residual filter。
  - 常量 false：不读取 final rest columns，直接输出空 block。
- 如果遇到 generated column placeholder 相关依赖，禁用 multi-stage LM。

这个阶段需要重点保证表达式语义一致，而不是自己重写 filter 判断逻辑。

### Stage C: 多阶段 LM reader

目标：实现 Stage 0 -> Stage 1 -> Final stage 的数据读取和拼接。

可以新增一个 reader，例如：

```cpp
class MultiStageLateMaterializationBlockInputStream;
```

或者在现有 `LateMaterializationBlockInputStream` 上扩展。为了降低对现有 LM 的影响，建议第一版新增独立实现。

核心成员：

- `stage0_filter_stream` 或现有 Stage 0 LM filter stream。
- `stage1_filter_stream`。
- `final_rest_stream`。
- residual filter actions。
- final schema projection。
- adaptive state。

核心 read 流程：

```text
read():
  stage0_block, stage0_filter = read_stage0_filter()
  if empty:
    return empty

  stage1_block = stage1_filter_stream.readWithFilter(stage0_filter)
  residual_filter = execute_residual_filters(stage1_block)

  update_adaptive_state(stage1_block.rows, countBytesInFilter(residual_filter))

  if should_use_late_mode(residual_filter):
    combined_filter = compose(stage0_filter, residual_filter)
    rest_block = final_rest_stream.readWithFilter(combined_filter)
    filterBlock(stage1_block, residual_filter)
    return hstack_and_project(stage1_block, rest_block)
  else:
    rest_block = final_rest_stream.readWithFilter(stage0_filter)
    full_block = hstack_and_project(stage1_block, rest_block)
    filterBlock(full_block, residual_filter)
    return full_block
```

需要保证：

- `stage1_filter_stream` 和 `final_rest_stream` 与 Stage 0 使用相同 segment snapshot、read ranges 和 block 切分。
- `readWithFilter` 传入的 filter 坐标必须匹配底层 reader 当前 block 的原始 rows。
- `residual_filter` 只在 Stage 0 输出坐标上使用。
- `combined_filter` 才能传给 final rest reader。
- hstack 前两个 block 的 rows 必须一致。

### Stage D: Adaptive mode

目标：避免 weak residual filters 导致性能回退。

改动点：

- 在多阶段 LM reader 中维护 adaptive state。
- Sampling 阶段累计：
  - `sample_stage0_rows`。
  - `sample_residual_passed_rows`。
  - `sample_blocks`。
- 达到采样阈值后：
  - 如果 residual filtered ratio > 0.5，进入 LateMode。
  - 否则进入 DirectMode。
- DirectMode 不新增 full stream，只是 final rest stream 使用 `stage0_filter`，然后内存中过 residual filter。
- LateMode 内部可以继续使用现有 per-block fallback，避免小 block 或弱 block 上不划算的 filtered read。

### Stage E: Runtime stats 和可观测性

目标：保证 TiDB 能看到 Selection execution summary，TableScan/Selection 的 `actRows` 能分别表示 Stage 0/Stage 1 输出行数，并能从 TiFlash 日志判断 adaptive direct/late 选择。

改动点：

- 在 `DAGStorageInterpreter::executeImpl(PipelineExecGroupBuilder & group_builder)` 的 pipeline path 中：
  - 如果 multi-stage LM 消费了 `filter_conditions`，跳过 `executePushedDownFilter(...)`。
  - 调用 `dag_context.addOperatorProfileInfos(filter_conditions.executor_id, group_builder.getCurProfileInfos(), true)`。
- 在 `DAGStorageInterpreter::generateSelectQueryInfos()` 中创建 `MultiStageLateMaterializationRuntimeStats`：
  - TableScan executor id 的 rows override 指向 `stage0_output_rows`。
  - Selection executor id 的 rows override 指向 `stage1_output_rows`。
- 在 `MultiStageLateMaterializationBlockInputStream` 中累加：
  - `effective_stage0_filter.passed_count` 到 `stage0_output_rows`。
  - `residual_passed_rows` 到 `stage1_output_rows`。
- 在 `ExecutorStatistics::collectRuntimeDetail()` 中应用 rows override，只覆盖 `base.rows`。
- 增加 scan 日志字段：
  - `multi_stage_lm_enabled`。
  - `adaptive_mode`。
  - `sample_stage0_rows`。
  - `sample_residual_passed_rows`。
  - `residual_filtered_ratio`。
  - `late_mode_blocks`：按 Stage 1 输入 block 统计，不按输出 block 统计。
  - `direct_mode_blocks`：按 Stage 1 输入 block 统计，不按输出 block 统计。
- 后续可以考虑加入 profile extra info，但第一版不是必须。

### Stage F: 测试和性能验证

目标：证明正确性，并验证优化收益和回退可控。

测试内容见下一节。

## 测试方案

### 单元测试

新增或扩展 DeltaMerge late materialization 相关 gtest：

- Stage 0 filter + residual filter 均命中。
- Stage 0 filter 命中，residual filter 全过滤。
- Stage 0 filter 命中，residual filter 全通过。
- residual filter 过滤率大于 50%，进入 LateMode。
- residual filter 过滤率小于等于 50%，进入 DirectMode。
- Stage 1 filter columns 同时也是输出列。
- Stage 1 filter columns 与 Stage 0 filter columns 重叠。
- residual filters 包含 NULL 判断。
- residual filters 包含 cast。
- residual filters 包含 timezone sensitive 的时间类型。
- 常量 true filter。
- 常量 false filter。
- generated columns 存在时禁用。

重点断言：

- 查询结果和普通 TableScan + Selection 一致。
- block schema 和列顺序与原始 pipeline 输出一致。
- `combined_filter` 和 `residual_filter` 坐标转换正确。
- hstack 后 rows 对齐。

### Fullstack 测试

新增 fullstack SQL 测试：

- 宽列查询，部分 filter 进入 TableScan pushed down filter，部分 filter 保留在 Selection。
- `EXPLAIN ANALYZE` 中 Selection 仍然有 execution summary。
- 结果与关闭 multi-stage LM 的结果一致。
- generated column 查询不启用。
- `keep_order` 查询不启用。

### Benchmark

构造宽表冷数据 benchmark：

- 输出列数：8、16、28、64。
- residual filter 过滤率：10%、30%、50%、70%、90%。
- 数据冷热：page cache warm、page cache cold。
- LIMIT/TopN 查询和无 TopN 查询都覆盖。

重点观察：

- `dtfile.data_scanned_rows`。
- `dtfile.tot_read`。
- `lm_skip_rows`。
- TableScan 和 Selection execution time。
- DirectMode 下的额外 CPU 和 hstack 开销。
- LateMode 下的磁盘读取减少幅度。

## 风险

### 性能回退

如果 residual filters 过滤率不高，多阶段 LM 会增加一次读取 filter columns 和 hstack 成本。通过 runtime adaptive 降级到 DirectMode 可以缓解，但 DirectMode 仍然比原始一阶段 LM 多一次 split read。

缓解：

- 静态启用规则要求 final rest columns 明显多于 stage1 filter columns。
- residual filtered ratio 必须大于 50% 才进入 LateMode。
- generated columns、keep_order 等复杂场景第一版禁用。

### filter 坐标错误

`stage0_filter` 是原始 block 坐标，`residual_filter` 是 Stage 0 输出坐标。Final rest reader 需要原始 block 坐标上的 filter。

缓解：

- 明确实现 `compose(stage0_filter, residual_filter)`。
- 单元测试覆盖稀疏 filter、全 true、全 false、交错 filter。
- 每次 compose 后断言 residual position 消耗完。

### 表达式语义不一致

如果 Stage 1 自己实现表达式逻辑，容易在 cast、timezone、collation、NULL 等语义上和普通 Selection 不一致。

缓解：

- 复用 `DAGExpressionAnalyzer` 和现有 Selection expression actions。
- 不手写表达式判断。
- generated columns 第一版禁用。

### Runtime stats 不严格

Selection 和 TableScan 可能共享同一组 execution summary，不严格反映各自独立耗时、bytes 和 loops。

缓解：

- 第一版保证 Selection 有 execution summary，并保证 TableScan/Selection 的 `actRows` 分别显示 Stage 0/Stage 1 输出行数。
- 后续如果需要更精确，可以在多阶段 LM reader 内维护独立 synthetic `OperatorProfileInfo`。

## 备选方案

### TopN 下推到 storage

最彻底的方案是把 TopN 下推到 TableScan 内部，先只读排序列和过滤列，算出最终 TopN 行，再读取这些行需要的宽列。

优点：

- 理论上可以把 final rest columns 降到只读 LIMIT 行。
- 对 `ORDER BY ... LIMIT` 宽表查询收益最大。

缺点：

- 需要 storage 线程之间合并 TopN。
- 需要 row reference 或二次读取能力。
- pipeline 中可能引入新的 barrier。
- 改动大，正确性和调度复杂度高。

第一版不选择该方案。

### force_push_down_all_filters_to_scan

把所有 filters 都放进 scan，可以减少 Selection 上方的过滤延迟。

缺点：

- 所有 filter columns 都必须在 late materialization 之前读取。
- 对宽 filter columns 或弱过滤率场景容易造成读放大。
- 不能解决 residual filters 和 final rest columns 分阶段读取的问题。

第一版不选择该方案。

### 引入 full_stream

Adaptive 发现 residual filters 弱后，可以切换到一个完整 `full_stream`，直接读取所有 columns 再执行 residual filters。

优点：

- DirectMode 下和原始执行方式最接近。

缺点：

- 需要维护额外 stream。
- 和当前 segment/block 对齐更复杂。
- 切换时需要处理已经采样读取过的部分。

第一版不选择该方案。DirectMode 使用 split read，但 final rest columns 按 `stage0_filter` 读取，然后内存中过 residual filter。

### 跨 stage 复用 Stage 0 block

可以保留 Stage 0 已经读取过的列，Stage 1 直接复用。

优点：

- 减少重复读取 filter columns。

缺点：

- 需要跨 stage 保存 block 和 offset。
- 容易引入复杂的 row alignment 状态。
- 对第一版收益有限，因为 Stage 0 filter columns 通常较窄且可能已经变热。

第一版不选择该方案。Stage 1 需要的列可以重新读取。

## 未解决问题

- 是否需要把 adaptive 参数暴露为 settings。
- 是否需要在 TiFlash runtime stats 中增加 multi-stage LM 的 extra info。
- 是否需要让 TiDB planner 感知该优化，并在 explain 中更明确地展示。
- 是否要在后续支持 generated columns。
- 是否要在后续实现 TopN pushdown 或 row reference based final column fetch。
