// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnUtils.h>
#include <Columns/ColumnsCommon.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/NullableUtils.h>

namespace DB
{

namespace
{
struct KeyColumns
{
    size_t index;
    const IColumn * column_ptr;
    bool is_nullable;
};
std::vector<KeyColumns> getKeyColumns(const Names & key_names, const Block & block)
{
    size_t keys_size = key_names.size();
    std::vector<KeyColumns> key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i].index = block.getPositionByName(key_names[i]);
        key_columns[i].column_ptr = block.getByPosition(key_columns[i].index).column.get();

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i].column_ptr->isColumnNullable())
        {
            key_columns[i].column_ptr
                = &static_cast<const ColumnNullable &>(*key_columns[i].column_ptr).getNestedColumn();
            key_columns[i].is_nullable = true;
        }
    }

    return key_columns;
}

bool canAsColumnString(const IColumn * column)
{
    return typeid_cast<const ColumnString *>(column)
        || (column->isColumnConst()
            && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(column)->getDataColumn()));
}

enum class StringCollatorKind
{
    StringBinary,
    StringBinaryPadding,
    String,
};

StringCollatorKind getStringCollatorKind(const TiDB::TiDBCollators & collators)
{
    if (collators.empty() || !collators[0])
        return StringCollatorKind::StringBinary;

    switch (collators[0]->getCollatorType())
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    {
        return StringCollatorKind::StringBinaryPadding;
    }
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    {
        return StringCollatorKind::StringBinary;
    }
    default:
    {
        // for CI COLLATION, use original way
        return StringCollatorKind::String;
    }
    }
}

} // namespace

HashJoin::HashJoin(
    const Names & key_names_left_,
    const Names & key_names_right_,
    ASTTableJoin::Kind kind_,
    const String & req_id,
    const NamesAndTypes & output_columns_,
    const TiDB::TiDBCollators & collators_,
    const JoinNonEqualConditions & non_equal_conditions_,
    const Settings & settings_)
    : kind(kind_)
    , join_req_id(req_id)
    , may_probe_side_expanded_after_join(mayProbeSideExpandedAfterJoin(kind))
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , collators(collators_)
    , non_equal_conditions(non_equal_conditions_)
    , settings(settings_)
    , log(Logger::get(join_req_id))
    , output_columns(output_columns_)
{
    RUNTIME_ASSERT(key_names_left.size() == key_names_right.size());
}

void HashJoin::initRowLayoutAndHashJoinMethod()
{
    size_t keys_size = key_names_right.size();
    if (keys_size == 0)
    {
        method = HashJoinKeyMethod::Cross;
        return;
    }

    auto key_columns = getKeyColumns(key_names_right, right_sample_block);
    RUNTIME_ASSERT(key_columns.size() == keys_size);
    /// Move all raw join key column to the front of the join key.
    Names new_key_names_left, new_key_names_right;
    BoolVec raw_key_flag(keys_size);
    bool is_all_key_fixed = true;
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (key_columns[i].column_ptr->valuesHaveFixedSize())
        {
            new_key_names_left.emplace_back(key_names_left[i]);
            new_key_names_right.emplace_back(key_names_right[i]);
            raw_key_flag[i] = true;
            row_layout.key_column_indexes.push_back(key_columns[i].index);
            row_layout.raw_key_column_indexes.push_back({key_columns[i].index, key_columns[i].is_nullable});
            row_layout.key_column_fixed_size += key_columns[i].column_ptr->sizeOfValueIfFixed();
            continue;
        }
        is_all_key_fixed = false;
        if (canAsColumnString(key_columns[i].column_ptr))
        {
            if (getStringCollatorKind(collators) == StringCollatorKind::StringBinary)
            {
                new_key_names_left.emplace_back(key_names_left[i]);
                new_key_names_right.emplace_back(key_names_right[i]);
                raw_key_flag[i] = true;
                row_layout.key_column_indexes.push_back(key_columns[i].index);
                row_layout.raw_key_column_indexes.push_back({key_columns[i].index, key_columns[i].is_nullable});
                continue;
            }
        }
    }
    if (is_all_key_fixed)
    {
        method = findFixedSizeJoinKeyMethod(keys_size, row_layout.key_column_fixed_size);
    }
    else if (canAsColumnString(key_columns[0].column_ptr))
    {
        switch (getStringCollatorKind(collators))
        {
        case StringCollatorKind::StringBinary:
            method = HashJoinKeyMethod::OneKeyStringBin;
            break;
        case StringCollatorKind::StringBinaryPadding:
            method = HashJoinKeyMethod::OneKeyStringBinPadding;
            break;
        case StringCollatorKind::String:
            method = HashJoinKeyMethod::OneKeyString;
            break;
        }
    }
    else
    {
        // E.g. Decimal256
        method = HashJoinKeyMethod::KeySerialized;
    }

    row_layout.next_pointer_offset = getHashValueByteSize(method);
    row_layout.join_key_offset = row_layout.next_pointer_offset + sizeof(RowPtr);
    row_layout.join_key_all_raw = row_layout.raw_key_column_indexes.size() == keys_size;

    for (size_t i = 0; i < keys_size; ++i)
    {
        if (!raw_key_flag[i])
        {
            row_layout.key_column_indexes.push_back(key_columns[i].index);
            /// For non-raw join key, the original join key column should be placed in `other_column_indexes`.
            row_layout.other_column_indexes.push_back({key_columns[i].index, false});
            new_key_names_left.emplace_back(key_names_left[i]);
            new_key_names_right.emplace_back(key_names_right[i]);
        }
    }
    key_names_left.swap(new_key_names_left);
    key_names_right.swap(new_key_names_right);

    std::unordered_set<std::string> key_names_right_set;
    for (auto & name : key_names_right)
        key_names_right_set.insert(name);

    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        auto & c = right_sample_block.getByPosition(i);
        if (!key_names_right_set.contains(c.name))
        {
            if (c.column->valuesHaveFixedSize())
            {
                row_layout.other_column_fixed_size += c.column->sizeOfValueIfFixed();
                row_layout.other_column_indexes.push_back({i, true});
            }
            else
            {
                row_layout.other_column_indexes.push_back({i, false});
            }
        }
    }
}

void HashJoin::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;

    right_sample_block = materializeBlock(sample_block);
    size_t num_columns_to_add = right_sample_block.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = right_sample_block.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(right_sample_block.getByPosition(i));

    initRowLayoutAndHashJoinMethod();

    build_concurrency = build_concurrency_;
    active_build_threads = build_concurrency;
    build_workers_data.resize(build_concurrency);
    for (size_t i = 0; i < build_concurrency; ++i)
        build_workers_data[i].key_getter = createHashJoinKeyGetter(method, collators);
    for (size_t i = 0; i < HJ_BUILD_PARTITION_COUNT + 1; ++i)
        multi_row_containers.emplace_back(std::make_unique<MultipleRowContainer>());
}

void HashJoin::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    left_sample_block = sample_block;

    probe_concurrency = probe_concurrency_;
    probe_workers_data.resize(probe_concurrency);
}

void HashJoin::insertFromBlock(const Block & b, size_t stream_index)
{
    RUNTIME_ASSERT(stream_index < build_concurrency);

    if unlikely (b.rows() == 0)
        return;

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    Block block = b;
    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;

    size_t columns = block.columns();
    for (size_t pos = 0; pos < columns;)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }

    assertBlocksHaveEqualStructure(block, right_sample_block, "Join Build");

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_right);

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// Reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, non_equal_conditions.right_filter_column, null_map_holder, null_map);

    columns = block.columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < columns; ++i)
    {
        ColumnPtr col = block.safeGetByPosition(i).column;
        if (ColumnPtr converted = col->convertToFullColumnIfConst())
            block.safeGetByPosition(i).column = converted;
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
    {
        for (size_t i = 0; i < columns; ++i)
            convertColumnToNullable(block.getByPosition(i));
    }

    if (!isCrossJoin(kind))
    {
        insertBlockToRowContainers(
            method,
            needRecordNotInsertRows(kind),
            block,
            key_columns,
            null_map,
            row_layout,
            multi_row_containers,
            build_workers_data[stream_index]);
    }
}

bool HashJoin::finishOneBuild(size_t stream_index)
{
    LOG_INFO(log, "{} convert block to rows cost {}ms", stream_index, build_workers_data[stream_index].convert_time);
    if (active_build_threads.fetch_sub(1) == 1)
    {
        workAfterBuildFinish();
        return true;
    }
    return false;
}

void HashJoin::workAfterBuildFinish()
{
    size_t all_build_row_count = 0;
    for (size_t i = 0; i < build_concurrency; ++i)
        all_build_row_count += build_workers_data[i].row_count;

    Stopwatch watch;
    pointer_table.init(all_build_row_count, settings.probe_enable_prefetch_threshold);

    LOG_INFO(
        log,
        "allocate pointer table cost {}ms, rows {}, pointer table size {}, added column num {}, enable prefetch {}",
        watch.elapsedMilliseconds(),
        all_build_row_count,
        pointer_table.getPointerTableSize(),
        right_sample_block.columns(),
        pointer_table.enableProbePrefetch());
}

bool HashJoin::buildPointerTable(size_t stream_index)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        return true;

#define M(METHOD)                                                                                         \
    case HashJoinKeyMethod::METHOD:                                                                       \
        using HashValueType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>::HashValueType; \
        return pointer_table.build<HashValueType##METHOD>(                                                \
            row_layout,                                                                                   \
            build_workers_data[stream_index],                                                             \
            multi_row_containers,                                                                         \
            settings.max_block_size);
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

Block HashJoin::joinBlock(JoinProbeContext & context, size_t stream_index)
{
    RUNTIME_ASSERT(stream_index < probe_concurrency);

    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;
    context.prepareForHashProbe(
        method,
        kind,
        key_names_left,
        non_equal_conditions.left_filter_column,
        probe_output_name_set,
        collators);

    std::vector<Block> result_blocks;
    size_t result_rows = 0;
    /// min_result_block_size is used to avoid generating too many small block, use 50% of the block size as the default value
    size_t min_result_block_size = std::max(1, (std::min(context.block.rows(), settings.max_block_size) + 1) / 2);
    while (true)
    {
        Block block = doJoinBlock(context, stream_index);
        assert(block);
        block = removeUselessColumn(block);
        result_rows += block.rows();
        result_blocks.push_back(std::move(block));
        /// exit the while loop if
        /// 1. probe_process_info.all_rows_joined_finish is true, which means all the rows in current block is processed
        /// 2. the block may be expanded after join and result_rows exceeds the min_result_block_size
        if (context.isCurrentProbeFinished()
            || (may_probe_side_expanded_after_join && result_rows >= min_result_block_size))
            break;
    }
    assert(!result_blocks.empty());
    return vstackBlocks(std::move(result_blocks));
};

Block HashJoin::doJoinBlock(JoinProbeContext & context, size_t stream_index)
{
    Block block = context.block;

    size_t num_columns_to_add = right_sample_block.columns();
    /// Add new columns to the block.
    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    RUNTIME_ASSERT(block.rows() >= context.start_row_idx);
    size_t remain_rows = block.rows() - context.start_row_idx;
    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = right_sample_block.getByPosition(i);
        RUNTIME_CHECK_MSG(
            !block.has(src_column.name),
            "block from probe side has a column with the same name: {} as a column in right_sample_block",
            src_column.name);

        added_columns.push_back(src_column.column->cloneEmpty());
        if (src_column.type && src_column.type->haveMaximumSizeOfValue())
        {
            // todo figure out more accurate `rows`
            added_columns.back()->reserve(remain_rows);
        }
    }

    JoinProbeParameter param(
        context,
        probe_workers_data[stream_index],
        method,
        kind,
        non_equal_conditions,
        settings,
        pointer_table,
        row_layout);

    joinProbeBlock(param, added_columns);

    return block;
}

Block HashJoin::removeUselessColumn(Block & block) const
{
    Block projected_block;
    for (const auto & name_and_type : output_columns_after_finalize)
    {
        auto & column = block.getByName(name_and_type.name);
        projected_block.insert(std::move(column));
    }
    return projected_block;
}

void HashJoin::finalize(const Names & parent_require [[maybe_unused]])
{
    if unlikely (finalized)
        return;

    finalized = true;
}

} // namespace DB