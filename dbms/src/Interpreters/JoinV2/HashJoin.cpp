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
#include <Common/Exception.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/NullableUtils.h>

#include "ext/scope_guard.h"

namespace DB
{

namespace
{
struct KeyColumn
{
    const IColumn * column_ptr;
    bool is_nullable;
};
std::vector<KeyColumn> getKeyColumns(const Names & key_names, const Block & block)
{
    size_t keys_size = key_names.size();
    std::vector<KeyColumn> key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i].column_ptr = block.getByName(key_names[i]).column.get();

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
    const Settings & settings_,
    const String & match_helper_name_)
    : kind(kind_)
    , join_req_id(req_id)
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , collators(collators_)
    , non_equal_conditions(non_equal_conditions_)
    , settings(settings_)
    , match_helper_name(match_helper_name_)
    , log(Logger::get(join_req_id))
    , has_other_condition(non_equal_conditions.other_cond_expr != nullptr)
    , output_columns(output_columns_)
{
    RUNTIME_ASSERT(key_names_left.size() == key_names_right.size());
    output_block = Block(output_columns);
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

    bool is_all_key_fixed = true;
    bool has_decimal_256 = false;
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (typeid_cast<const ColumnDecimal<Decimal256> *>(key_columns[i].column_ptr))
        {
            has_decimal_256 = true;
            continue;
        }
        if (key_columns[i].column_ptr->valuesHaveFixedSize())
            row_layout.key_column_fixed_size += key_columns[i].column_ptr->sizeOfValueIfFixed();
        else
            is_all_key_fixed = false;
    }
    if (has_decimal_256)
    {
        method = HashJoinKeyMethod::KeySerialized;
    }
    else if (is_all_key_fixed)
    {
        method = findFixedSizeJoinKeyMethod(keys_size, row_layout.key_column_fixed_size);
    }
    else if (keys_size == 1 && canAsColumnString(key_columns[0].column_ptr))
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
        method = HashJoinKeyMethod::KeySerialized;
    }

    std::unordered_set<size_t> raw_required_key_index_set;
    if (method != HashJoinKeyMethod::KeySerialized)
    {
        /// Move all raw join key column to the end of the join key.
        Names new_key_names_left, new_key_names_right;
        BoolVec raw_required_key_flag(keys_size);
        for (size_t i = 0; i < keys_size; ++i)
        {
            bool is_raw_required = false;
            if (key_columns[i].column_ptr->valuesHaveFixedSize())
            {
                if (right_sample_block_pruned.has(key_names_right[i]))
                    is_raw_required = true;
            }
            else
            {
                if (canAsColumnString(key_columns[i].column_ptr)
                    && getStringCollatorKind(collators) == StringCollatorKind::StringBinary
                    && right_sample_block_pruned.has(key_names_right[i]))
                {
                    is_raw_required = true;
                }
            }
            if (is_raw_required)
            {
                size_t index = right_sample_block_pruned.getPositionByName(key_names_right[i]);
                /// If this index has already existed in set, do not move it to the end of the join key.
                if (!raw_required_key_index_set.contains(index))
                {
                    raw_required_key_flag[i] = true;
                    raw_required_key_index_set.insert(index);
                    row_layout.raw_required_key_column_indexes.push_back({index, key_columns[i].is_nullable});
                    continue;
                }
            }
            new_key_names_left.emplace_back(key_names_left[i]);
            new_key_names_right.emplace_back(key_names_right[i]);
        }

        for (size_t i = 0; i < keys_size; ++i)
        {
            if (raw_required_key_flag[i])
            {
                new_key_names_left.emplace_back(key_names_left[i]);
                new_key_names_right.emplace_back(key_names_right[i]);
            }
        }
        key_names_left.swap(new_key_names_left);
        key_names_right.swap(new_key_names_right);
    }

    size_t columns = right_sample_block_pruned.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        if (raw_required_key_index_set.contains(i))
            continue;
        auto & c = right_sample_block_pruned.getByPosition(i);
        if (c.column->valuesHaveFixedSize())
        {
            row_layout.other_column_fixed_size += c.column->sizeOfValueIfFixed();
            row_layout.other_required_column_indexes.push_back({i, true});
        }
        else
        {
            row_layout.other_required_column_indexes.push_back({i, false});
        }
    }
}

void HashJoin::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    if unlikely (initialized)
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

    /// In case of LEFT and FULL joins, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(right_sample_block.getByPosition(i));

    right_sample_block_pruned = right_sample_block;
    removeUselessColumn(right_sample_block_pruned);

    initRowLayoutAndHashJoinMethod();

    build_concurrency = build_concurrency_;
    active_build_worker = build_concurrency;
    build_workers_data.resize(build_concurrency);
    for (size_t i = 0; i < build_concurrency; ++i)
        build_workers_data[i].key_getter = createHashJoinKeyGetter(method, collators);
    for (size_t i = 0; i < JOIN_BUILD_PARTITION_COUNT + 1; ++i)
        multi_row_containers.emplace_back(std::make_unique<MultipleRowContainer>());
}

void HashJoin::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    left_sample_block = sample_block;

    probe_concurrency = probe_concurrency_;
    active_probe_worker = probe_concurrency;
    probe_workers_data.resize(probe_concurrency);
}

void HashJoin::insertFromBlock(const Block & b, size_t stream_index)
{
    RUNTIME_ASSERT(stream_index < build_concurrency);

    if unlikely (b.rows() == 0)
        return;

    if unlikely (!initialized)
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    Stopwatch watch;

    Block block = b;
    size_t rows = block.rows();

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_right);
    /// Some useless columns maybe key columns so they must be removed after extracting key columns.
    removeUselessColumn(block);

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// Reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, non_equal_conditions.right_filter_column, null_map_holder, null_map);

    size_t columns = block.columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    block = materializeBlock(block);

    /// In case of LEFT and FULL joins, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < columns; ++i)
            convertColumnToNullable(block.getByPosition(i));

    assertBlocksHaveEqualStructure(block, right_sample_block_pruned, "Join Build");

    if (!isCrossJoin(kind))
    {
        insertBlockToRowContainers(
            method,
            needRecordNotInsertRows(kind),
            block,
            rows,
            key_columns,
            null_map,
            row_layout,
            multi_row_containers,
            build_workers_data[stream_index]);
    }

    build_workers_data[stream_index].build_time += watch.elapsedMilliseconds();
}

bool HashJoin::finishOneBuild(size_t stream_index)
{
    auto & wd = build_workers_data[stream_index];
    LOG_DEBUG(
        log,
        "{} insert block to row containers cost {}ms, row count {}, padding size {}({:.2f}% of all size {})",
        stream_index,
        wd.build_time,
        wd.row_count,
        wd.padding_size,
        100.0 * wd.padding_size / wd.all_size,
        wd.all_size);
    if (active_build_worker.fetch_sub(1) == 1)
    {
        workAfterBuildFinish();
        return true;
    }
    return false;
}

bool HashJoin::finishOneProbe(size_t stream_index)
{
    auto & wd = probe_workers_data[stream_index];
    LOG_DEBUG(
        log,
        "{} probe handle {} rows, cost {}ms(hash_table {}ms + replicate {}ms + other condition {}ms), collision {}",
        stream_index,
        wd.probe_handle_rows,
        wd.probe_time / 1000000UL,
        wd.probe_hash_table_time / 1000000UL,
        wd.replicate_time / 1000000UL,
        wd.other_condition_time / 1000000UL,
        wd.collision);
    return active_probe_worker.fetch_sub(1) == 1;
}

void HashJoin::workAfterBuildFinish()
{
    size_t all_build_row_count = 0;
    for (size_t i = 0; i < build_concurrency; ++i)
        all_build_row_count += build_workers_data[i].row_count;

    bool enable_tagged_pointer = settings.enable_tagged_pointer;
    for (size_t i = 0; i < build_concurrency; ++i)
        enable_tagged_pointer &= build_workers_data[i].enable_tagged_pointer;

    Stopwatch watch;
    pointer_table.init(
        method,
        all_build_row_count,
        getHashValueByteSize(method),
        settings.probe_enable_prefetch_threshold,
        enable_tagged_pointer);

    LOG_DEBUG(
        log,
        "allocate pointer table cost {}ms, rows {}, pointer table size {}, added column num {}, enable prefetch {}, "
        "enable tagged pointer {}",
        watch.elapsedMilliseconds(),
        all_build_row_count,
        pointer_table.getPointerTableSize(),
        right_sample_block_pruned.columns(),
        pointer_table.enableProbePrefetch(),
        pointer_table.enableTaggedPointer());
}

bool HashJoin::buildPointerTable(size_t stream_index)
{
    bool is_end;
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        return true;

#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        if constexpr (KeyGetterType##METHOD::Type::joinKeyCompareHashFirst())              \
            is_end = pointer_table.build<KeyGetterType##METHOD::HashValueType>(            \
                build_workers_data[stream_index],                                          \
                multi_row_containers,                                                      \
                settings.max_block_size);                                                  \
        else                                                                               \
            is_end = pointer_table.build<void>(                                            \
                build_workers_data[stream_index],                                          \
                multi_row_containers,                                                      \
                settings.max_block_size);                                                  \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }

    if (is_end)
    {
        auto & wd = build_workers_data[stream_index];
        LOG_INFO(
            log,
            "{} build pointer table finish cost {}ms, build rows {}",
            stream_index,
            wd.build_pointer_table_time,
            wd.build_pointer_table_size);
    }
    return is_end;
}

Block HashJoin::joinBlock(JoinProbeContext & context, size_t stream_index)
{
    RUNTIME_ASSERT(stream_index < probe_concurrency);

    Stopwatch watch;
    SCOPE_EXIT({ probe_workers_data[stream_index].probe_time += watch.elapsedFromLastTime(); });

    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;
    context.prepareForHashProbe(
        method,
        kind,
        key_names_left,
        non_equal_conditions.left_filter_column,
        probe_output_name_set,
        collators,
        row_layout);

    Block block = doJoinBlock(context, stream_index);
    assert(block);
    block = removeUselessColumnForOutput(block);
    auto & wd = probe_workers_data[stream_index];
    if (context.isCurrentProbeFinished())
        wd.probe_handle_rows += context.rows;

    size_t rows = block.rows();
    if (rows == 0 || rows >= settings.max_block_size || output_block_after_finalize.columns() == 0)
        return block;

    if (!wd.result_block_buffer)
    {
        wd.result_block_buffer = std::move(block);
        for (auto & column : wd.result_block_buffer)
            column.column->assumeMutable()->reserve(settings.max_block_size);
        return output_block_after_finalize;
    }

    size_t current_rows = wd.result_block_buffer.rows();
    size_t clone_rows = std::min(settings.max_block_size - current_rows, rows);
    size_t columns = block.columns();
    assert(columns == wd.result_block_buffer.columns());
    for (size_t i = 0; i < columns; ++i)
        wd.result_block_buffer.getByPosition(i).column->assumeMutable()->insertRangeFrom(
            *block.getByPosition(i).column.get(),
            0,
            clone_rows);

    if (current_rows + clone_rows >= settings.max_block_size)
    {
        Block res = std::move(wd.result_block_buffer);

        if (clone_rows < rows)
        {
            wd.result_block_buffer = output_block_after_finalize.cloneEmpty();
            for (auto & column : wd.result_block_buffer)
                column.column->assumeMutable()->reserve(settings.max_block_size);

            for (size_t i = 0; i < columns; ++i)
                wd.result_block_buffer.getByPosition(i).column->assumeMutable()->insertRangeFrom(
                    *block.getByPosition(i).column.get(),
                    clone_rows,
                    rows - clone_rows);
        }
        return res;
    }

    return output_block_after_finalize;
}

Block HashJoin::doJoinBlock(JoinProbeContext & context, size_t stream_index)
{
    Block block = context.block;
    size_t rows = context.rows;
    size_t existing_columns = block.columns();

    size_t num_columns_to_add = right_sample_block_pruned.columns();
    /// Add new columns to the block.
    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    RUNTIME_ASSERT(rows >= context.start_row_idx);
    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = right_sample_block_pruned.getByPosition(i);
        RUNTIME_CHECK_MSG(
            !block.has(src_column.name),
            "block from probe side has a column with the same name: {} as a column in right_sample_block",
            src_column.name);

        added_columns.push_back(src_column.column->cloneEmpty());
        if (src_column.type && src_column.type->haveMaximumSizeOfValue())
            added_columns.back()->reserveAlign(rows, FULL_VECTOR_SIZE_AVX2);
    }

    auto & wd = probe_workers_data[stream_index];
    Stopwatch watch;
    joinProbeBlock(context, wd, method, kind, non_equal_conditions, settings, pointer_table, row_layout, added_columns);
    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    for (size_t index = 0; index < num_columns_to_add; ++index)
    {
        const ColumnWithTypeAndName & sample_col = right_sample_block_pruned.getByPosition(index);
        block.insert(ColumnWithTypeAndName(std::move(added_columns[index]), sample_col.type, sample_col.name));
    }

    if likely (rows > 0)
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto mutable_column = block.safeGetByPosition(i).column->cloneEmpty();
            mutable_column->insertSelectiveFrom(*block.safeGetByPosition(i).column.get(), wd.selective_offsets);
            block.safeGetByPosition(i).column = std::move(mutable_column);
        }
    }
    wd.replicate_time += watch.elapsedFromLastTime();

    if (has_other_condition)
    {
        handleOtherConditions(block, stream_index);
        wd.other_condition_time += watch.elapsedFromLastTime();
    }

    return block;
}

void HashJoin::removeUselessColumn(Block & block) const
{
    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;
    for (size_t pos = 0; pos < block.columns();)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }
}

Block HashJoin::removeUselessColumnForOutput(const Block & block) const
{
    // remove useless columns and adjust the order of columns
    Block projected_block;
    for (const auto & name_and_type : output_columns_after_finalize)
    {
        const auto & column = block.getByName(name_and_type.name);
        projected_block.insert(std::move(column));
    }
    return projected_block;
}

void HashJoin::finalize(const Names & parent_require)
{
    if unlikely (finalized)
        return;
    /// finalize will do 3 things
    /// 1. update expected_output_schema
    /// 2. set expected_output_schema_for_other_condition
    /// 3. generated needed input columns
    NameSet required_names_set;
    for (const auto & name : parent_require)
        required_names_set.insert(name);
    if unlikely (!match_helper_name.empty() && !required_names_set.contains(match_helper_name))
    {
        /// should only happen in some tests
        required_names_set.insert(match_helper_name);
    }
    for (const auto & name_and_type : output_columns)
    {
        if (required_names_set.contains(name_and_type.name))
        {
            output_columns_after_finalize.push_back(name_and_type);
            output_column_names_set_after_finalize.insert(name_and_type.name);
        }
    }
    RUNTIME_CHECK_MSG(
        output_column_names_set_after_finalize.size() == output_columns_after_finalize.size(),
        "Logical error, the output of join contains duplicated columns");

    output_block_after_finalize = Block(output_columns_after_finalize);
    Names updated_require;
    if (match_helper_name.empty())
        updated_require = parent_require;
    else
    {
        required_names_set.erase(match_helper_name);
        for (const auto & name : required_names_set)
            updated_require.push_back(name);
    }
    if (!non_equal_conditions.null_aware_eq_cond_name.empty())
    {
        updated_require.push_back(non_equal_conditions.null_aware_eq_cond_name);
    }
    if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
        updated_require.push_back(non_equal_conditions.other_eq_cond_from_in_name);
    if (!non_equal_conditions.other_cond_name.empty())
        updated_require.push_back(non_equal_conditions.other_cond_name);
    auto keep_used_input_columns
        = !isCrossJoin(kind) && (isNullAwareSemiFamily(kind) || isSemiFamily(kind) || isLeftOuterSemiFamily(kind));
    /// nullaware/semi join will reuse the input columns so need to let finalize keep the input columns
    if (non_equal_conditions.null_aware_eq_cond_expr != nullptr)
    {
        non_equal_conditions.null_aware_eq_cond_expr->finalize(updated_require, keep_used_input_columns);
        updated_require = non_equal_conditions.null_aware_eq_cond_expr->getRequiredColumns();
    }
    if (non_equal_conditions.other_cond_expr != nullptr)
    {
        non_equal_conditions.other_cond_expr->finalize(updated_require, keep_used_input_columns);
        updated_require = non_equal_conditions.other_cond_expr->getRequiredColumns();
    }

    if (non_equal_conditions.other_cond_expr != nullptr || non_equal_conditions.null_aware_eq_cond_expr != nullptr)
    {
        output_columns_names_set_for_other_condition_after_finalize = output_column_names_set_after_finalize;
        for (const auto & name : updated_require)
            output_columns_names_set_for_other_condition_after_finalize.insert(name);
        if (!match_helper_name.empty())
            output_columns_names_set_for_other_condition_after_finalize.insert(match_helper_name);
    }

    /// remove duplicated column
    required_names_set.clear();
    for (const auto & name : updated_require)
        required_names_set.insert(name);
    /// add some internal used columns
    if (!non_equal_conditions.left_filter_column.empty())
        required_names_set.insert(non_equal_conditions.left_filter_column);
    if (!non_equal_conditions.right_filter_column.empty())
        required_names_set.insert(non_equal_conditions.right_filter_column);
    /// add join key to required_columns
    for (const auto & name : key_names_right)
        required_names_set.insert(name);
    for (const auto & name : key_names_left)
        required_names_set.insert(name);

    for (const auto & name : required_names_set)
        required_columns.push_back(name);
    finalized = true;
}

void HashJoin::handleOtherConditions(Block & block, size_t stream_index) const
{
    /// save block_rows because block.rows() returns the first column's size, after other_cond_expr->execute(block),
    /// some column maybe removed, and the first column maybe the match_helper_column which does not have the same size
    /// as the other columns
    auto block_rows = block.rows();
    non_equal_conditions.other_cond_expr->execute(block);

    auto & filter = probe_workers_data[stream_index].filter_column->getData();
    filter.clear();
    filter.reserve(block_rows);
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);

    removeUselessColumn(block);
    size_t result_size_hint = countBytesInFilter(filter);
    for (size_t i = 0; i < block.columns(); ++i)
        block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, result_size_hint);
}

} // namespace DB
