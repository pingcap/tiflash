// Copyright 2023 PingCAP, Ltd.
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

#include <Functions/FunctionHelpers.h>
#include <Interpreters/CrossJoinProbeHelper.h>

namespace DB
{
namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder;

template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>
{
    static size_t calTotalRightRows(const BlocksList & blocks)
    {
        size_t total_rows = 0;
        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            total_rows += rows_right;
        }
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            total_rows = std::min(total_rows, 1);
        return total_rows;
    }
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        const BlocksList & blocks,
        IColumn::Filter *,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join [[maybe_unused]],
        size_t total_right_rows,
        size_t max_block_size)
    {
        size_t current_size = dst_columns[0]->size();
        if (current_size > 0 && current_size + total_right_rows > max_block_size)
            return true;

        size_t expanded_row_size = 0;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], i, total_right_rows);

        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                rows_right = std::min(rows_right, 1);
            }

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right = block_right.getByPosition(col_num).column.get();
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(*column_right, 0, rows_right);
            }
            expanded_row_size += rows_right;
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                if (expanded_row_size >= 1)
                    break;
            }
        }
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            (*expanded_row_size_after_join)[i] = current_offset + expanded_row_size;
        current_offset += expanded_row_size;
        return false;
    }
    static bool addNotFound(
        MutableColumns & /* dst_columns */,
        size_t /* num_existing_columns */,
        ColumnRawPtrs & /* src_left_columns */,
        size_t /* num_columns_to_add */,
        size_t i [[maybe_unused]],
        IColumn::Filter * /* is_row_matched */,
        IColumn::Offset & current_offset [[maybe_unused]],
        IColumn::Offsets * expanded_row_size_after_join [[maybe_unused]],
        size_t /*max_block_size*/)
    {
        /// for inner all/any join, just skip this row
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            (*expanded_row_size_after_join)[i] = current_offset;
        return false;
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, STRICTNESS>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        const BlocksList & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t total_right_rows,
        size_t max_block_size)
    {
        return CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            total_right_rows,
            max_block_size);
    }
    static bool addNotFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        IColumn::Filter * /* is_row_matched */,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join [[maybe_unused]],
        size_t max_block_size)
    {
        size_t current_size = dst_columns[0]->size();
        if (current_size > 0 && current_size + 1 > max_block_size)
            return true;
        /// for left all/any join, mark this row as matched
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            (*expanded_row_size_after_join)[i] = 1 + current_offset;
        current_offset += 1;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertFrom(*src_left_columns[col_num], i);
        for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            dst_columns[num_existing_columns + col_num]->insertDefault();
        return false;
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::Any>
{
    static bool addFound(
        MutableColumns & /* dst_columns */,
        size_t /* num_existing_columns */,
        ColumnRawPtrs & /* src_left_columns */,
        size_t /* num_columns_to_add */,
        size_t i,
        const BlocksList & /* blocks */,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & /* current_offset */,
        IColumn::Offsets * /* expanded_row_size_after_join */,
        size_t /* total_right_rows */,
        size_t /*max_block_size*/)
    {
        (*is_row_matched)[i] = 0;
        return false;
    }
    static bool addNotFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, ASTTableJoin::Strictness::Any>::addNotFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            max_block_size);
        if (!ret)
            (*is_row_matched)[i] = 1;
        return ret;
    }
    static bool allRightRowsMaybeAdded()
    {
        return false;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::All>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        const BlocksList & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t total_right_rows,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            total_right_rows,
            max_block_size);
        if (!ret)
            (*is_row_matched)[i] = 0;
        return ret;
    }
    static bool addNotFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, ASTTableJoin::Strictness::All>::addNotFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            max_block_size);
        if (!ret)
            (*is_row_matched)[i] = 1;
        return ret;
    }
    static bool allRightRowsMaybeAdded()
    {
        return true;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuterSemi, STRICTNESS>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        const BlocksList & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t total_right_rows,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add - 1,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            total_right_rows,
            max_block_size);
        if (!ret)
            dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_1);
        return ret;
    }
    static bool addNotFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        size_t i,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, STRICTNESS>::addNotFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add - 1,
            i,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            max_block_size);
        if (!ret)
            dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_0);
        return ret;
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
Block crossProbeBlockImpl(
    ProbeProcessInfo & probe_process_info,
    const BlocksList & right_blocks)
{
    size_t num_existing_columns = probe_process_info.block.columns();
    size_t num_columns_to_add = probe_process_info.result_block_schema.columns() - num_existing_columns;

    ColumnRawPtrs src_left_columns(num_existing_columns);
    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = probe_process_info.block.getByPosition(i).column.get();
    }

    std::vector<size_t> right_column_index;
    for (size_t i = 0; i < num_columns_to_add; ++i)
        right_column_index.push_back(num_existing_columns + i);

    auto total_right_rows = CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::calTotalRightRows(right_blocks);

    size_t current_row = probe_process_info.start_row;
    size_t block_rows = probe_process_info.block.rows();
    MutableColumns dst_columns(probe_process_info.result_block_schema.columns());
    size_t reserved_rows = (block_rows - current_row) * total_right_rows;
    for (size_t i = 0; i < probe_process_info.result_block_schema.columns(); ++i)
    {
        dst_columns[i] = probe_process_info.result_block_schema.getByPosition(i).column->cloneEmpty();
        if likely (reserved_rows > 0)
            dst_columns[i]->reserve(reserved_rows);
    }

    IColumn::Offset current_offset = 0;
    bool block_full;
    auto * filter_ptr = probe_process_info.filter.get();
    auto * offset_ptr = probe_process_info.offsets_to_replicate.get();

    for (; current_row < block_rows; ++current_row)
    {
        if constexpr (has_null_map)
        {
            if ((*probe_process_info.null_map)[current_row])
            {
                /// filter out by left_conditions, so just treated as not joined column
                block_full = CrossJoinAdder<KIND, STRICTNESS>::addNotFound(
                    dst_columns,
                    num_existing_columns,
                    src_left_columns,
                    num_columns_to_add,
                    current_row,
                    filter_ptr,
                    current_offset,
                    offset_ptr,
                    probe_process_info.max_block_size);
                if (block_full)
                    break;
                continue;
            }
        }
        if (total_right_rows > 0)
        {
            block_full = CrossJoinAdder<KIND, STRICTNESS>::addFound(
                dst_columns,
                num_existing_columns,
                src_left_columns,
                num_columns_to_add,
                current_row,
                right_blocks,
                filter_ptr,
                current_offset,
                offset_ptr,
                total_right_rows,
                probe_process_info.max_block_size);
        }
        else
        {
            block_full = CrossJoinAdder<KIND, STRICTNESS>::addNotFound(
                dst_columns,
                num_existing_columns,
                src_left_columns,
                num_columns_to_add,
                current_row,
                filter_ptr,
                current_offset,
                offset_ptr,
                probe_process_info.max_block_size);
        }
        if (block_full)
            break;
    }
    probe_process_info.end_row = current_row;
    probe_process_info.all_rows_joined_finish = (probe_process_info.end_row == block_rows);
    return probe_process_info.result_block_schema.cloneWithColumns(std::move(dst_columns));
}
} // namespace

Block crossProbeBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const BlocksList & right_blocks)
{
    Block block{};

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
#define DISPATCH(HAS_NULL_MAP)                                                                                 \
    if (kind == Cross && strictness == All)                                                                    \
        block = crossProbeBlockImpl<Cross, All, HAS_NULL_MAP>(probe_process_info, right_blocks);               \
    else if (kind == Cross && strictness == Any)                                                               \
        block = crossProbeBlockImpl<Cross, Any, HAS_NULL_MAP>(probe_process_info, right_blocks);               \
    else if (kind == Cross_LeftOuter && strictness == All)                                                     \
        block = crossProbeBlockImpl<Cross_LeftOuter, All, HAS_NULL_MAP>(probe_process_info, right_blocks);     \
    else if (kind == Cross_LeftOuter && strictness == Any)                                                     \
        block = crossProbeBlockImpl<Cross_LeftOuter, Any, HAS_NULL_MAP>(probe_process_info, right_blocks);     \
    else if (kind == Cross_Anti && strictness == All)                                                          \
        block = crossProbeBlockImpl<Cross_Anti, All, HAS_NULL_MAP>(probe_process_info, right_blocks);          \
    else if (kind == Cross_Anti && strictness == Any)                                                          \
        block = crossProbeBlockImpl<Cross_Anti, Any, HAS_NULL_MAP>(probe_process_info, right_blocks);          \
    else if (kind == Cross_LeftOuterSemi && strictness == All)                                                 \
        block = crossProbeBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_LeftOuterSemi && strictness == Any)                                                 \
        block = crossProbeBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_LeftOuterAnti && strictness == All)                                                 \
        block = crossProbeBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_LeftOuterAnti && strictness == Any)                                                 \
        block = crossProbeBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else                                                                                                       \
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    if (probe_process_info.null_map)
    {
        DISPATCH(true)
    }
    else
    {
        DISPATCH(false)
    }
#undef DISPATCH

    return block;
}

} // namespace DB
