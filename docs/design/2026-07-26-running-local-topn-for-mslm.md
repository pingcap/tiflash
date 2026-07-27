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
- [ORDER BY 列对齐](#order-by-列对齐)
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
- 第一版不支持没有进入现有 MSLM 路径的 `TopN + TableScan`。例如没有 TableScan pushed filter，或者没有 residual Selection 的查询，后续单独扩展。
- 不对旧的非 pipeline DAG executor 路径做支持。
- 不追求和未优化路径在 ORDER BY ties 下选择完全相同的物理 rows。

## MVP 范围

第一版只支持下面范围：

- 只支持已经满足现有 MSLM 启用条件的查询。也就是说，TableScan 必须已有 pushed-down filters 作为 Stage 0，且 TableScan 上方必须有 residual Selection 作为 Stage 1。
- 只支持 `ORDER BY` plain columns，即 TiPB `ByItem.expr` 必须直接是 `ColumnRef`。
- 只支持 constant `LIMIT` 和 constant `OFFSET`。
- 只支持 TiDB 下发的单表 `TopN -> Selection -> TableScan`。在 TiFlash physical plan 中，这一层 `Selection` 必须已经被合并到 `PhysicalTableScan::filter_conditions`，所以第一版实际只识别 `PhysicalTopN -> PhysicalTableScan(with filter_conditions and pushed_down_filters)`。
- 第一版不穿透 `PhysicalFilter`。如果 TiFlash physical plan 中仍然存在独立的 `PhysicalFilter -> PhysicalTableScan`，说明 residual Selection 没有进入当前 MSLM Stage 1 识别范围，TopN-enhanced MSLM 直接禁用。
- 不移除上层 TopN。
- 只在 MSLM 已经满足启用条件时启用。
- TopN filter 只用于减少 final rest columns 的读取。

后续可以分阶段扩展：

- 阶段二支持有 TableScan pushed filter 但没有 residual Selection 的 `TopN -> TableScan`。这要求把 residual filter 变成 optional，Stage 1 只读取 order-by columns。
- 阶段三支持完全没有 filter 的 `TopN -> TableScan`。这需要把 MSLM 从当前的 filter-driven 模型泛化为 order-by-column-driven 模型，Stage 1 order-by stream 需要成为 block driver，工程风险更高。

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

当前 MSLM 已经不再使用 adaptive mode。TopN-enhanced MSLM 因此不需要根据 residual 过滤率在 direct/late 两种策略之间做启发式选择，而是只保留下面三个自然分支：

- `residual_filter && topn_candidate_filter` 全过滤时，skip final rest stream。
- `residual_filter && topn_candidate_filter` 全通过时，final rest stream 按 Stage 0 filter 直接读取。
- 其他部分通过场景，把 Stage 1 坐标的 filter compose 回 Stage 0 原始坐标，再用 combined filter 读取 final rest columns。

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
Selection
  |
TableScan
```

识别成功后，生成一个 MSLM TopN 描述：

```text
struct MSLMTopNDescription
{
    SortDescription storage_sort_description;
    UInt64 topk; // limit + offset
    ColumnDefines order_by_columns;
};
```

第一版 `order_by_columns` 只允许 plain columns，不构造复杂 expression actions。

Plan 识别建议在 `PhysicalTopN` 和 `PhysicalTableScan` 之间完成。`PhysicalTopN` 只在 child 直接是 `PhysicalTableScan` 时尝试生成 MSLM TopN 描述；如果 child 是 `PhysicalFilter`、Projection、Join、Aggregation、Window 等其他节点，则不启用该优化。上层 `PhysicalTopN` executor 仍然保留，storage 内描述只用于减少 final rest columns 的读取。

对于 TiPB DAG 中的单层 `TopN -> Selection -> TableScan`，TiFlash physical planner 会先尝试把 `Selection` 合并到 `PhysicalTableScan::filter_conditions`。因此第一版实际需要识别的 physical plan 形态是：

```text
PhysicalTopN
  |
PhysicalTableScan(with filter_conditions and pushed_down_filters)
```

这里的 `filter_conditions` 是 TiFlash physical planner 内部保存的 residual Selection。对于 TiPB DAG 中形态为 `Selection -> TableScan` 的单个 Selection，TiFlash 构建 physical plan 时会把它放入 `PhysicalTableScan::filter_conditions`，而不是保留一个单独的 `PhysicalFilter`。如果 Selection 的 child 不是 TableScan，或者同一个 TableScan 已经设置过 `filter_conditions`，则会保留普通 `PhysicalFilter`，第一版不处理这种情况。

需要注意，`PhysicalTableScan::filter_conditions` 不等于 TiPB `TableScan.pushed_down_filter_conditions`：

- `TableScan.pushed_down_filter_conditions` 是现有 MSLM 的 Stage 0 pushed filter。
- `PhysicalTableScan::filter_conditions` 是 Stage 1 residual Selection。
- 第一版 TopN-enhanced MSLM 要求这两者都存在。

### ORDER BY 列对齐

即使第一版只支持 plain column，也需要显式处理 TopN order-by column 和 DeltaMerge storage column 的对齐。

不要直接使用 `PhysicalTopN` 现有 `SortDescription.column_name` 去匹配 storage column。这个名字来自 TopN child schema，而 DeltaMerge Stage 1 block 使用的是 `ColumnDefine.name`。TableScan 后还有 schema projection、特殊列名和 cast-after-TS 等逻辑，直接按 name 对齐容易和 storage header 不一致。

第一版采用基于 column id 的对齐流程：

```text
tipb::ByItem.expr() 必须是 ColumnRef
  -> decode ColumnRef index
  -> table_scan.getColumns()[index].id
  -> 在 columns_to_read 或 table column defines 中按 column id 找到 ColumnDefine
  -> 将该 ColumnDefine 加入 Stage 1 columns
  -> 使用 ColumnDefine.name 构造 storage-side SortDescription
```

`MSLMTopNDescription::storage_sort_description` 中的 column name 必须是 Stage 1 block 中真实存在的 storage column name，而不是 TopN child schema name。

Stage 1 column union 和 final rest column subtraction 都必须按 `ColumnID` 去重：

```text
stage1_columns = residual_filter_columns union order_by_columns
final_rest_columns = final_columns_to_read - stage1_columns
```

如果 order-by column 已经属于 residual filter columns，则 Stage 1 只读一次。如果 order-by column 也是最终输出列，则 final rest columns 中必须排除该列，避免重复读取。

第一版不支持没有 residual Selection 的 `TopN -> TableScan`。这种场景没有 Selection executor 可以承载 candidate rows 的 `EXPLAIN ANALYZE` 展示，也需要让 residual filter 变成 optional，后续单独设计。

plain column 还需要保证 storage 内比较语义和上层 TopN 一致。第一版建议保守禁用下面场景：

- order-by expr 不是 direct `ColumnRef`。
- order-by column 是 generated column 或 virtual `_tidb_tid`。
- order-by column 需要 TableScan 后的 extra cast 才能得到上层 TopN 的比较值。
- order-by column 是 variable-length string 且暂未确认 collator 和 owned key 内存上界。
- order-by column 不在第一版支持的类型白名单内。

第一版 order-by column 类型白名单为：

- integer numeric types。
- decimal types。
- date/datetime types。
- float types。

第一版明确不支持：

- `String` / `FixedString` / `Bytes`，因为需要处理 collation 和 owned key 内存上界。
- `Timestamp`，因为它通常需要 TableScan 后的 timezone extra cast，storage 内底层值不一定等于上层 TopN 的比较值。
- `Time` / `Duration`，除非后续明确确认它和上层 TopN 使用完全一致的底层比较表示。
- JSON、Enum/Set、Bit、Vector、Array、Tuple、Map 等其他类型。

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

Heap entry 只能记录轻量信息，不能持有 `Block` 或 column 引用：

```text
struct HeapEntry
{
    OwnedSortKey key;
    UInt64 stream_sequence;
    UInt64 block_sequence;
    UInt32 row_index_in_stage1_block;
};
```

`block_sequence` 用于判断被 eviction 的 entry 是否属于当前 block。`row_index_in_stage1_block` 只在 entry 属于当前 block 时有效。

如果 eviction 的 entry 来自当前 block，可以将 `topn_candidate_filter[row_index_in_stage1_block]` 置为 false，避免对该 row 读取 final rest columns。

如果 eviction 的 entry 来自历史 block，则不做任何回撤。历史 block 已经输出给上层 TopN，该 row 只是额外候选。

### Heap 内存模型

Running local TopN 不能保存 `{Block, row_id}`。如果 heap entry 持有 `Block` 或 column memory 引用，会导致历史 blocks 被 retain，内存随扫描推进累积。

Heap 中只保存排序需要的 owned key：

```text
heap memory = O(topk * order_key_size)
current block filter memory = O(current_block_rows)
```

不保存：

- 历史 block。
- 历史 row payload。
- 历史 column 引用。
- 用于回读的 row locator。

历史 candidate rows 一旦输出给上层 TopN，MSLM stream 不再负责保存它们。Heap eviction 只释放 owned sort key。

对于第一版支持的 order-by 类型，owned key 只拷贝 fixed-size value 和 NULL flag。对于 variable-length 类型，例如 String，owned key 可能带来较大内存开销，并且需要处理 collation。第一版禁用 variable-length order-by columns；后续如果要支持，可以再增加 per-stream memory guard 或 owned collation key。

如果采用 memory guard，触发后应退化为普通 MSLM：

```text
后续 blocks 不再生成 TopN candidate filter。
final rest columns 按现有 MSLM 逻辑读取。
上层 TopN 保留，保证最终结果正确。
```

Ties 也需要避免破坏内存上界。第一版 heap 仍最多保存 `topk` 个 entries。与 heap worst 相等的 rows 可以作为当前 block candidate 输出，但不插入 heap。这样 ties 较多时剪枝效果会下降，但 heap 内存保持 bounded。

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
    PriorityQueue<HeapEntry> heap; // heap top is current worst candidate
    UInt64 current_block_sequence;
};
```

Heap 大小最多为 `topk`。如果 `topk` 超过启发式阈值，则禁用该优化。

比较器必须和上层 TopN 使用的 comparator 保持一致。第一版只支持 plain order-by columns，减少 comparator 语义不一致风险。

第一版不要求 heap entry 能定位历史 block。Heap entry 的 locator 只服务于 current block eviction。历史 entry 被 eviction 时只释放 key，不访问历史 block。

### Sort Key 表示

第一版不使用 order-preserving bytes key。TiFlash 现有 sort 路径本身也是基于 `IColumn::compareAt()` / typed compare 直接比较 column values，而不是先构造可按 bytes 比较的 sort key。引入 bytes key 需要重新设计 signed integer、float、NaN、DESC、NULL、decimal 等类型的 order-preserving encoding，正确性风险较高。

第一版也不直接使用通用 `Field` 作为 heap key。`Field` 虽然已经是 owned variant，但它支持 String/Array/Tuple 等当前不需要的类型，大小和通用抽取路径都偏重；float 的 NaN 比较语义也需要额外绕开。为了降低 hot path 开销，第一版使用一个更窄的 typed sort key：

```text
static constexpr size_t max_sort_key_columns = 4;

enum class SortKeyKind
{
    Int64,
    UInt64,
    Float32,
    Float64,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Date,
    DateTime,
};

struct SortKeyField
{
    bool is_null;
    SortKeyKind kind;
    TypedFixedSizeValue value;
};

struct OwnedSortKey
{
    std::array<SortKeyField, max_sort_key_columns> fields;
    size_t size;
};
```

`TypedFixedSizeValue` 是 conceptual storage，实际实现可以使用 union 或其他 fixed-size typed storage。它只需要覆盖第一版白名单内的 fixed-size values，不支持 String/Array/Tuple。

`RunningLocalTopN` 本身不模板化。每个 order-by column 在初始化时生成一个 descriptor，descriptor 中保存 typed extractor 和 comparator：

```text
struct SortKeyColumnDesc
{
    String column_name;
    size_t column_pos;
    SortKeyKind kind;
    int direction;

    extract_owned(column, row, SortKeyField &);
    compare_owned(SortKeyField, SortKeyField);
    compare_column_with_owned(column, row, SortKeyField);
};
```

模板只用于生成具体类型的 extractor/comparator。这样一个 `OwnedSortKey` 可以同时包含多个不同类型的 order-by columns，而不需要把整个 `RunningLocalTopN` 或 `SortKeyField` 做成模板。

### Sort Key 比较语义

NULL 在所有支持类型中统一处理：

```text
NULL is minimum before applying direction.
ASC  -> NULL first
DESC -> NULL last
```

非 NULL values 的比较规则：

- integer numeric、date、datetime 直接比较底层整数值。
- decimal 直接比较底层 decimal value。因为 lhs/rhs 来自同一个 order-by column，decimal type 和 scale 一致。
- float 使用 TiFlash 现有 `CompareHelper<Float32/Float64>::compare(..., nan_direction_hint=-1)` 语义，避免 NaN 行为和上层 sort 不一致。

每个字段先得到不带 direction 的比较结果，再乘以 `SortKeyColumnDesc::direction`。多列 ORDER BY 按顺序比较，直到第一个非 0 结果。

### Lazy Owned Key Materialization

第一版不对 residual passed 的每一行都构造 `OwnedSortKey`。处理当前 `stage1_block` 时，可以直接使用 order-by columns 参与比较；只有当前 row 真的需要进入 heap 时，才 lazy materialize owned key。

处理一个 block 前，先从 `stage1_block` 中收集 order-by column 指针：

```text
sort_columns[i] = stage1_block.getByPosition(desc[i].column_pos).column.get()
```

per-row hot path 不再从 `Block` 里按 name 或 position 查找 column，而是直接使用预先收集好的 `IColumn *`：

```text
compareRowWithOwnedKey(sort_columns, row, heap.worst().key)
materializeOwnedKey(sort_columns, row)
```

heap 中始终只保存 owned key，不保存 `Block`、`IColumn *` 或 historical row reference。当前 row 只有在 `update()` 函数内部比较时可以直接读 `stage1_block` column。这样既避免 retain historical blocks，又避免给明显不可能进入 local TopK 的 rows 做 key copy。

当前 block 内如果某个 entry 被后续更优 row eviction，可以通过 `block_sequence` 和 `row_index_in_stage1_block` 将对应的 `topn_candidate_filter` 置为 false。历史 block 的 entry 被 eviction 时不能回撤，因为 rows 已经输出给上层 TopN。

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
- 查询包含 TopN，且 TopN 的 child 直接是单表 `PhysicalTableScan`。
- `PhysicalTableScan` 已经包含 residual `filter_conditions`，并且 TableScan 已经包含 pushed-down filters。
- TopN 上方仍保留全局 TopN executor。
- TopN 的 `LIMIT` 和 `OFFSET` 是常量。
- `topk = limit + offset` 未超过阈值。
- TopN 之前的 Selection predicates 全部在 MSLM Stage 0 或 Stage 1 内执行。
- ORDER BY 只包含 direct `ColumnRef` plain columns。
- ORDER BY columns 能通过 `ColumnRef` index 映射到 `table_scan.getColumns()` 中的 column id，并能进一步映射到 DeltaMerge `ColumnDefine`。
- ORDER BY columns 不需要 TableScan 后 extra cast。
- ORDER BY columns 类型属于第一版白名单：integer numeric、decimal、date、datetime、float。
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
- ORDER BY plain column 无法按 column id 映射到 storage column。
- ORDER BY column 需要 TableScan 后 extra cast。
- ORDER BY column 类型不在第一版白名单内，例如 String、FixedString、Timestamp、Time/Duration、JSON、Enum/Set、Bit、Vector、Array、Tuple、Map。
- ORDER BY 依赖 generated column。
- TopN 包含非常大的 offset。
- TopN 之前仍有无法进入 MSLM 的 Selection。
- TopN 的 child 是 `PhysicalFilter`，即 residual Selection 没有被合并到 `PhysicalTableScan::filter_conditions`。
- 查询没有 residual Selection，或者没有 TableScan pushed-down filters。
- 查询要求 keep order 且和 MSLM 现有限制冲突。
- TopN 位于 join、aggregation、window 等算子之上。

## 实现计划

### Step 1: 文档和测试用例设计

- 明确 MVP 范围。
- 增加基于 DAG request 的单测设计。
- 明确 ties、offset、NULL ordering、float NaN、以及 unsupported string/collation 的禁用测试覆盖。

### Step 2: Pattern 识别和元数据传递

- 在 `PhysicalTopN` 构建阶段识别 child 直接是 `PhysicalTableScan` 的模式。
- 只在 `PhysicalTableScan` 同时包含 residual `filter_conditions` 和 TableScan pushed-down filters 时启用。
- 如果 child 是 `PhysicalFilter` 或其他节点，第一版不尝试穿透或重写。
- 构造 `MSLMTopNDescription`。
- 保留上层 TopN executor，不修改最终 DAG 语义。

### Step 3: ORDER BY ColumnRef 对齐和 Stage 1 列集合

- 要求每个 `ByItem.expr` 都是 direct `ColumnRef`。
- 按 `ColumnRef` index -> `table_scan.getColumns()[index].id` -> `ColumnDefine` 的顺序对齐 order-by columns。
- 禁用无法按 column id 映射到 storage column 的 order-by columns。
- 禁用 generated column、virtual `_tidb_tid`、需要 TableScan 后 extra cast 的 order-by columns。
- 校验 order-by column 类型属于第一版白名单。
- 将 order-by columns 按 ColumnID 合并到 MSLM Stage 1 column set。
- final rest columns 按 ColumnID 排除 Stage 1 columns，避免 order-by columns 重复读取。
- 使用 Stage 1 storage column names 构造 storage-side order-by descriptor。

### Step 4: Typed Sort Key Infrastructure

- 实现 fixed-size typed `SortKeyField` / `OwnedSortKey`。
- 实现 `SortKeyColumnDesc`，保存 column position、direction、type kind、extractor 和 comparator。
- 支持 integer numeric、decimal、date、datetime、float。
- Nullable wrapper 统一由 extractor/comparator 处理，nested type 必须在白名单内。
- integer numeric、date、datetime 直接比较底层整数值。
- decimal 直接比较底层 decimal value。
- float comparator 使用现有 `CompareHelper` 的 NaN 语义。
- 不支持 String/FixedString/collation，不使用 order-preserving bytes key，不直接使用通用 `Field` 作为 heap key。

### Step 5: RunningLocalTopN 状态和算法

- 实现 per-stream bounded TopK heap，heap top 是当前 worst candidate。
- heap entry 只保存 owned key、stream/block sequence 和 current-block row index。
- 每个 block 开始时预先收集 order-by column pointers，per-row compare 不从 `Block` 查找 column。
- 支持 row-to-owned lazy compare，只在 row 需要进入 heap 时 materialize owned key。
- 支持 ASC/DESC、NULL ordering 和 TiFlash 现有 comparator 语义。
- 生成当前 block 的 `topn_candidate_filter`。
- 支持 current block eviction，将被当前 block 后续更优 row 挤出的 candidate filter 清掉。
- 历史 block entry eviction 不做回撤。

### Step 6: 接入 MSLM final rest 读取

- 在 residual filter 之后执行 running local TopN。
- 将 `residual_filter` 和 `topn_candidate_filter` 合并。
- 使用 combined filter 读取 final rest columns。
- 当 `residual_filter && topn_candidate_filter` 全过滤时，skip final rest stream。
- 当 `residual_filter && topn_candidate_filter` 全通过时，final rest stream 按 Stage 0 filter 直接读取。
- 当 `residual_filter && topn_candidate_filter` 部分通过时，compose 回 Stage 0 原始坐标并读取 final rest columns。

### Step 7: Runtime Stats 和可观测性

- 保留 `stage1_output_rows` internal counter，表示 residual Selection 通过的真实行数。
- 增加 `topn_candidate_rows` internal counter，表示 `residual_filter && topn_candidate_filter` 之后实际读取 final rest columns 的行数。
- 在 TopN-enhanced MSLM 启用时，可以用 `topn_candidate_rows` overwrite residual Selection executor 的 `actRows`，让 `EXPLAIN ANALYZE` 直接展示 final rest columns materialized rows。
- 该 overwrite 会改变 Selection `actRows` 的展示语义：它不再表示纯 residual Selection 后的逻辑行数，而是表示 storage 内输出给上层 TopN 的 candidate rows。
- TableScan executor 的 `actRows` 仍然使用 `stage0_output_rows`。
- 可选增加 debug log，输出 residual passed rows、TopN candidate rows、heap size、filtered rows。

## 测试方案

### 功能测试

- `ORDER BY c ASC LIMIT k`。
- `ORDER BY c DESC LIMIT k`。
- `ORDER BY c ASC LIMIT offset, count`。
- `WHERE + ORDER BY + LIMIT`，其中 WHERE 同时包含 Stage 0 pushed filter 和 Stage 1 residual filter。
- ORDER BY column 不在最终 projection 中。
- ORDER BY column 同时属于 residual filter columns。
- ORDER BY column 同时属于最终输出列，验证 final rest columns 不重复读取该列。
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
- ORDER BY plain column 按 column id 对齐到 storage column，避免依赖 child schema name。
- ORDER BY column 需要 TableScan 后 extra cast 时禁用优化。
- 没有 residual Selection 时禁用优化。
- 没有 TableScan pushed-down filters 时禁用优化。

### 回归测试

- MSLM 原有测试必须全部通过。
- TopN 上方的 projection 仍然正确。
- TopN-enhanced MSLM 启用时，EXPLAIN ANALYZE 中 residual Selection `actRows` 展示 TopN candidate rows，即 final rest columns materialized rows。
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
- 对 ASC/DESC、NULL、float NaN、decimal/date/datetime 做正确性单测。
- 对 String/FixedString/collation 做禁用路径单测。

### ORDER BY 列对齐错误

如果直接用 TopN child schema name 匹配 DeltaMerge storage column name，可能因为 TableScan projection、特殊列名或 cast-after-TS 造成错配。

缓解：

- 只支持 direct `ColumnRef`。
- 使用 `ColumnRef` index 映射到 `table_scan.getColumns()[index].id`。
- 后续所有 Stage 1 column union、final rest column subtraction 和 storage-side sort description 都基于 `ColumnID` / `ColumnDefine` 构造。
- 禁用无法按 column id 映射到 storage column 的场景。

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

### Heap 持有历史 Block 导致内存累积

如果 heap entry 设计成 `{Block, row_id}`，或者持有历史 column memory 引用，会导致已经处理过的 blocks 无法释放，内存随扫描推进累积。

缓解：

- Heap entry 只保存 owned sort key。
- Heap entry 不持有 `Block`、column 引用或历史 row payload。
- Row locator 只用于 current block 内 eviction。
- 历史 block entry 被 eviction 时只释放 key，不访问历史 block。
- 对 variable-length order-by keys 使用禁用策略或 memory guard。

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
- 第一版是否禁用 variable-length order-by columns，还是实现 per-stream memory guard。
- 是否需要支持 ORDER BY expression，以及 expression action 如何复用。
- 是否需要在 EXPLAIN ANALYZE 中暴露 TopN-enhanced MSLM 的 candidate rows。
- 是否需要在 cost model 中引入列宽和 TopN selectivity。
