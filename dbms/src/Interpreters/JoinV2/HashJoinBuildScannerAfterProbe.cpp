// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/HashJoinBuildScannerAfterProbe.h>
#include <Interpreters/NullableUtils.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

using enum ASTTableJoin::Kind;

JoinBuildScannerAfterProbe::JoinBuildScannerAfterProbe(HashJoin * join)
    : join(join)
{
    join_key_getter = createHashJoinKeyGetter(join->method, join->collators);
    ColumnRawPtrs key_columns
        = extractAndMaterializeKeyColumns(join->right_sample_block, materialized_key_columns, join->key_names_right);
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    resetHashJoinKeyGetter(join->method, join_key_getter, key_columns, join->row_layout);

    size_t left_columns = join->left_sample_block_pruned.columns();

    auto kind = join->kind;
    bool need_row_data;
    bool need_other_block_data;
    if (kind == RightOuter)
    {
        need_row_data = true;
        need_other_block_data = false;
    }
    else
    {
        need_row_data = false;
        for (auto [column_index, is_nullable] : join->row_layout.raw_key_column_indexes)
        {
            auto output_index = join->output_column_indexes.at(left_columns + column_index);
            need_row_data |= output_index >= 0;
            if (need_row_data)
                break;
        }
        for (auto [column_index, _] : join->row_layout.other_column_indexes)
        {
            auto output_index = join->output_column_indexes.at(left_columns + column_index);
            need_row_data |= output_index >= 0;
            if (need_row_data)
                break;
        }

        need_other_block_data = (kind == RightSemi || kind == RightAnti)
            && join->row_layout.other_column_indexes.size() > join->row_layout.other_column_count_for_other_condition;

        // The output data should not be empty
        RUNTIME_CHECK(need_row_data || need_other_block_data);
    }

#define SET_FUNC_PTR(KeyGetter, JoinType, need_row_data, need_other_block_data)                                 \
    {                                                                                                           \
        scan_func_ptr                                                                                           \
            = &JoinBuildScannerAfterProbe::scanImpl<KeyGetter, JoinType, need_row_data, need_other_block_data>; \
    }

#define CALL2(KeyGetter, JoinType)                         \
    {                                                      \
        if (need_row_data && need_other_block_data)        \
            SET_FUNC_PTR(KeyGetter, JoinType, true, true)  \
        else if (need_row_data)                            \
            SET_FUNC_PTR(KeyGetter, JoinType, true, false) \
        else                                               \
            SET_FUNC_PTR(KeyGetter, JoinType, false, true) \
    }

#define CALL(KeyGetter)                                                                         \
    {                                                                                           \
        if (kind == RightOuter)                                                                 \
            SET_FUNC_PTR(KeyGetter, RightOuter, true, false)                                    \
        else if (kind == RightSemi)                                                             \
            CALL2(KeyGetter, RightSemi)                                                         \
        else if (kind == RightAnti)                                                             \
            CALL2(KeyGetter, RightAnti)                                                         \
        else                                                                                    \
            throw Exception(                                                                    \
                fmt::format(                                                                    \
                    "Logical error: unknown combination of JOIN {} during scanning build side", \
                    magic_enum::enum_name(kind)),                                               \
                ErrorCodes::LOGICAL_ERROR);                                                     \
    }

    switch (join->method)
    {
#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        CALL(KeyGetterType##METHOD);                                                       \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception(
            fmt::format("Unknown JOIN keys variant {} during scanning build side", magic_enum::enum_name(join->method)),
            ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }

#undef CALL
#undef CALL2
#undef SET_FUNC_PTR
}

Block JoinBuildScannerAfterProbe::scan(JoinProbeWorkerData & wd)
{
    return (this->*scan_func_ptr)(wd);
}

template <typename KeyGetter, ASTTableJoin::Kind kind, bool need_row_data, bool need_other_block_data>
Block JoinBuildScannerAfterProbe::scanImpl(JoinProbeWorkerData & wd)
{
    static_assert(need_row_data || need_other_block_data);

    if (wd.is_scan_end)
        return {};

    using KeyGetterType = typename KeyGetter::Type;
    using HashValueType = typename KeyGetter::HashValueType;
    const auto & multi_row_containers = join->multi_row_containers;
    const size_t max_block_size = join->settings.max_block_size;
    const size_t left_columns = join->left_sample_block_pruned.columns();
    auto & non_joined_blocks = join->non_joined_blocks;

    auto & key_getter = *static_cast<KeyGetterType *>(join_key_getter.get());
    constexpr size_t key_offset
        = sizeof(RowPtr) + (KeyGetterType::joinKeyCompareHashFirst() ? sizeof(HashValueType) : 0);

    Block * full_block = non_joined_blocks.getNextFullBlock();
    if (full_block != nullptr)
        return *full_block;

    size_t scan_size = 0;
    RowContainer * container = wd.current_container;
    size_t index = wd.current_container_index;
    wd.selective_offsets.clear();
    wd.selective_offsets.reserve(max_block_size);
    constexpr size_t insert_batch_max_size = 256;
    wd.insert_batch.clear();
    wd.insert_batch.reserve(insert_batch_max_size);
    join->initOutputBlock(wd.scan_result_block);

    Block * non_joined_non_full_block = nullptr;
    size_t output_columns = wd.scan_result_block.columns();
    while (true)
    {
        non_joined_non_full_block = non_joined_blocks.getNextNonFullBlock();
        if (non_joined_non_full_block == nullptr)
            break;
        RUNTIME_CHECK(non_joined_non_full_block->columns() == output_columns);
        size_t rows = non_joined_non_full_block->rows();
        if (rows >= max_block_size / 2)
            return *non_joined_non_full_block;

        if (wd.scan_result_block.rows() + rows > max_block_size)
        {
            Block res_block;
            res_block.swap(wd.scan_result_block);

            join->initOutputBlock(wd.scan_result_block);
            for (size_t i = 0; i < output_columns; ++i)
            {
                auto & src_column = non_joined_non_full_block->getByPosition(i);
                auto & des_column = wd.scan_result_block.getByPosition(i);
                des_column.column->assumeMutable()->insertRangeFrom(*src_column.column, 0, rows);
            }
            return res_block;
        }

        for (size_t i = 0; i < output_columns; ++i)
        {
            auto & src_column = non_joined_non_full_block->getByPosition(i);
            auto & des_column = wd.scan_result_block.getByPosition(i);
            des_column.column->assumeMutable()->insertRangeFrom(*src_column.column, 0, rows);
        }
    }

    size_t scan_block_rows = wd.scan_result_block.rows();
    if constexpr (need_row_data)
        scan_block_rows += wd.insert_batch.size();
    else
        scan_block_rows += wd.selective_offsets.size();

    do
    {
        if (container == nullptr)
        {
            if (wd.current_scan_table_index != -1)
                container = multi_row_containers[wd.current_scan_table_index]->getScanNext();
            if (container == nullptr)
            {
                std::unique_lock lock(scan_build_lock);
                for (size_t i = 0; i < JOIN_BUILD_PARTITION_COUNT; ++i)
                {
                    scan_build_index = (scan_build_index + i) % JOIN_BUILD_PARTITION_COUNT;
                    container = multi_row_containers[scan_build_index]->getScanNext();
                    if (container != nullptr)
                    {
                        wd.current_scan_table_index = scan_build_index;
                        scan_build_index = (scan_build_index + 1) % JOIN_BUILD_PARTITION_COUNT;
                        break;
                    }
                }
            }
            if unlikely (container == nullptr)
            {
                wd.is_scan_end = true;
                break;
            }
        }
        size_t rows = container->size();
        size_t original_index = index;
        while (index < rows)
        {
            RowPtr ptr = container->getRowPtr(index);
            bool need_output;
            if constexpr (kind == RightSemi)
                need_output = hasRowPtrMatchedFlag(ptr);
            else
                need_output = !hasRowPtrMatchedFlag(ptr);
            if (need_output)
            {
                if constexpr (need_row_data)
                {
                    const auto & key = key_getter.deserializeJoinKey(ptr + key_offset);
                    size_t required_offset = key_offset + key_getter.getRequiredKeyOffset(key);
                    wd.insert_batch.push_back(ptr + required_offset);
                    if unlikely (wd.insert_batch.size() >= insert_batch_max_size)
                        flushInsertBatch<false>(wd);
                }
                if constexpr (need_other_block_data)
                {
                    wd.selective_offsets.push_back(index);
                }
                ++scan_block_rows;
                if unlikely (scan_block_rows >= max_block_size)
                {
                    ++index;
                    break;
                }
            }
            ++index;
        }

        if constexpr (need_other_block_data)
        {
            size_t other_columns = join->row_layout.other_column_indexes.size()
                - join->row_layout.other_column_count_for_other_condition;
            RUNTIME_CHECK(container->other_column_block.columns() == other_columns);
            for (size_t i = 0; i < other_columns; ++i)
            {
                size_t column_index
                    = join->row_layout.other_column_indexes[join->row_layout.other_column_count_for_other_condition + i]
                          .first;
                auto output_index = join->output_column_indexes.at(left_columns + column_index);
                // These columns must be in the final output schema otherwise they should be pruned
                RUNTIME_CHECK(output_index >= 0);
                auto & src_column = container->other_column_block.safeGetByPosition(i);
                auto & des_column = wd.scan_result_block.safeGetByPosition(output_index);
                des_column.column->assumeMutable()->insertSelectiveFrom(*src_column.column, wd.selective_offsets);
            }
            wd.selective_offsets.clear();
        }

        scan_size += index - original_index;

        if (index >= rows)
        {
            container = nullptr;
            index = 0;
        }

        if unlikely (scan_block_rows >= max_block_size)
            break;
    } while (scan_size < 2 * max_block_size);

    flushInsertBatch<true>(wd);
    fillNullMapWithZero(wd);

    wd.current_container = container;
    wd.current_container_index = index;

    if constexpr (kind == RightOuter)
    {
        for (size_t i = 0; i < output_columns; ++i)
        {
            auto des_mut_column = wd.scan_result_block.getByPosition(i).column->assumeMutable();
            size_t current_rows = des_mut_column->size();
            if (current_rows < scan_block_rows)
            {
                // This column should be nullable and from the left side
                RUNTIME_CHECK_MSG(
                    des_mut_column->isColumnNullable(),
                    "Column with name {} is not nullable",
                    wd.scan_result_block.getByPosition(i).name);
                auto & nullable_column = static_cast<ColumnNullable &>(*des_mut_column);
                nullable_column.insertManyDefaults(scan_block_rows - current_rows);
            }
        }
    }

    if unlikely (wd.is_scan_end || scan_block_rows >= max_block_size)
    {
        if (scan_block_rows == 0)
            return {};

        Block res_block;
        res_block.swap(wd.scan_result_block);
        return res_block;
    }
    return join->output_block_after_finalize;
}

template <bool last_flush>
void JoinBuildScannerAfterProbe::flushInsertBatch(JoinProbeWorkerData & wd) const
{
    const size_t left_columns = join->left_sample_block_pruned.columns();
    for (auto [column_index, is_nullable] : join->row_layout.raw_key_column_indexes)
    {
        auto output_index = join->output_column_indexes.at(left_columns + column_index);
        if (output_index < 0)
        {
            join->right_sample_block_pruned.safeGetByPosition(column_index)
                .column->deserializeAndAdvancePos(wd.insert_batch);
            continue;
        }
        auto & des_column = wd.scan_result_block.safeGetByPosition(output_index);
        IColumn * column = des_column.column->assumeMutable().get();
        if (is_nullable)
            column = &static_cast<ColumnNullable &>(*column).getNestedColumn();
        column->deserializeAndInsertFromPos(wd.insert_batch, true);
        if constexpr (last_flush)
            column->flushNTAlignBuffer();
    }

    size_t other_column_count;
    if (join->kind == RightOuter)
        other_column_count = join->row_layout.other_column_indexes.size();
    else
        other_column_count = join->row_layout.other_column_count_for_other_condition;

    const size_t invalid_start_offset = other_column_count;
    size_t advance_start_offset = invalid_start_offset;
    for (size_t i = 0; i < other_column_count; ++i)
    {
        size_t column_index = join->row_layout.other_column_indexes[i].first;
        auto output_index = join->output_column_indexes.at(left_columns + column_index);
        if (output_index < 0)
        {
            advance_start_offset = std::min(advance_start_offset, i);
            continue;
        }
        if (advance_start_offset != invalid_start_offset)
        {
            while (advance_start_offset < i)
            {
                size_t column_index = join->row_layout.other_column_indexes[advance_start_offset].first;
                join->right_sample_block_pruned.safeGetByPosition(column_index)
                    .column->deserializeAndAdvancePos(wd.insert_batch);
                ++advance_start_offset;
            }
            advance_start_offset = invalid_start_offset;
        }
        auto & des_column = wd.scan_result_block.safeGetByPosition(output_index);
        des_column.column->assumeMutable()->deserializeAndInsertFromPos(wd.insert_batch, true);
        if constexpr (last_flush)
            des_column.column->assumeMutable()->flushNTAlignBuffer();
    }

    wd.insert_batch.clear();
}

void JoinBuildScannerAfterProbe::fillNullMapWithZero(JoinProbeWorkerData & wd) const
{
    size_t left_columns = join->left_sample_block_pruned.columns();
    for (auto [column_index, is_nullable] : join->row_layout.raw_key_column_indexes)
    {
        auto output_index = join->output_column_indexes.at(left_columns + column_index);
        if (!is_nullable || output_index < 0)
            continue;

        auto des_mut_column = wd.scan_result_block.safeGetByPosition(output_index).column->assumeMutable();
        RUNTIME_CHECK(des_mut_column->isColumnNullable());
        auto & nullable_column = static_cast<ColumnNullable &>(*des_mut_column);
        size_t data_size = nullable_column.getNestedColumn().size();
        size_t nullmap_size = nullable_column.getNullMapColumn().size();
        RUNTIME_CHECK(nullmap_size <= data_size);
        nullable_column.getNullMapColumn().getData().resize_fill_zero(data_size);
    }
}

} // namespace DB
