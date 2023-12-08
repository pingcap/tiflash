// Copyright 2023 PingCAP, Inc.
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
#include <Interpreters/ProbeProcessInfo.h>

namespace DB
{
namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder;

template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter *,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join [[maybe_unused]],
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        size_t current_size = dst_columns[0]->size();
        if (current_size > 0 && current_size + right_rows_to_be_added > max_block_size)
            return true;

        size_t expanded_row_size = 0;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], i, right_rows_to_be_added);

        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                rows_right = std::min(rows_right, 1);
            }

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right
                    = block_right.getByPosition(right_column_index_in_right_block[col_num]).column.get();
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
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, STRICTNESS>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        return CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            right_column_index_in_right_block,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            right_rows_to_be_added,
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
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::Any>
{
    static bool addFound(
        MutableColumns & /* dst_columns */,
        size_t /* num_existing_columns */,
        ColumnRawPtrs & /* src_left_columns */,
        size_t /* num_columns_to_add */,
        std::vector<size_t> & /* right_column_index_in_right_block */,
        size_t /* i */,
        const Blocks & /* blocks */,
        IColumn::Filter * /* is_row_matched */,
        IColumn::Offset & /* current_offset */,
        IColumn::Offsets * /* expanded_row_size_after_join */,
        size_t /* right_rows_to_be_added */,
        size_t /*max_block_size*/)
    {
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
        return CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, ASTTableJoin::Strictness::Any>::addNotFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            max_block_size);
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
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            right_column_index_in_right_block,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            right_rows_to_be_added,
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
            (*is_row_matched)[i] = 1; // NOLINT
        return ret;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Semi, ASTTableJoin::Strictness::Any>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        return CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::Any>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            right_column_index_in_right_block,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            right_rows_to_be_added,
            max_block_size);
        ;
    }
    static bool addNotFound(
        MutableColumns & /* dst_columns */,
        size_t /* num_existing_columns */,
        ColumnRawPtrs & /* src_left_columns */,
        size_t /* num_columns_to_add */,
        size_t /* i */,
        IColumn::Filter * /* is_row_matched */,
        IColumn::Offset & /* current_offset */,
        IColumn::Offsets * /* expanded_row_size_after_join */,
        size_t /*max_block_size*/)
    {
        return false;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Semi, ASTTableJoin::Strictness::All>
{
    static bool addFound(
        MutableColumns & dst_columns,
        size_t num_existing_columns,
        ColumnRawPtrs & src_left_columns,
        size_t num_columns_to_add,
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        return CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            right_column_index_in_right_block,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            right_rows_to_be_added,
            max_block_size);
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
        return CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addNotFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add,
            i,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            max_block_size);
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
        const std::vector<size_t> & right_column_index_in_right_block,
        size_t i,
        const Blocks & blocks,
        IColumn::Filter * is_row_matched,
        IColumn::Offset & current_offset,
        IColumn::Offsets * expanded_row_size_after_join,
        size_t right_rows_to_be_added,
        size_t max_block_size)
    {
        auto ret = CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(
            dst_columns,
            num_existing_columns,
            src_left_columns,
            num_columns_to_add - 1,
            right_column_index_in_right_block,
            i,
            blocks,
            is_row_matched,
            current_offset,
            expanded_row_size_after_join,
            right_rows_to_be_added,
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
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
Block crossProbeBlockDeepCopyRightBlockImpl(ProbeProcessInfo & probe_process_info, const Blocks & right_blocks)
{
    size_t num_existing_columns = probe_process_info.cross_join_data->left_column_index_in_left_block.size();
    size_t num_columns_to_add = probe_process_info.cross_join_data->right_column_index_in_right_block.size();

    ColumnRawPtrs src_left_columns(num_existing_columns);
    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = probe_process_info.block
                                  .getByPosition(probe_process_info.cross_join_data->left_column_index_in_left_block[i])
                                  .column.get();
    }

    size_t current_row = probe_process_info.start_row;
    size_t block_rows = probe_process_info.block.rows();
    MutableColumns dst_columns(probe_process_info.cross_join_data->result_block_schema.columns());
    size_t reserved_rows = std::min(
        (block_rows - current_row) * probe_process_info.cross_join_data->right_rows_to_be_added_when_matched,
        probe_process_info.max_block_size);
    for (size_t i = 0; i < probe_process_info.cross_join_data->result_block_schema.columns(); ++i)
    {
        dst_columns[i] = probe_process_info.cross_join_data->result_block_schema.getByPosition(i).column->cloneEmpty();
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
        if (probe_process_info.cross_join_data->right_rows_to_be_added_when_matched > 0)
        {
            block_full = CrossJoinAdder<KIND, STRICTNESS>::addFound(
                dst_columns,
                num_existing_columns,
                src_left_columns,
                num_columns_to_add,
                probe_process_info.cross_join_data->right_column_index_in_right_block,
                current_row,
                right_blocks,
                filter_ptr,
                current_offset,
                offset_ptr,
                probe_process_info.cross_join_data->right_rows_to_be_added_when_matched,
                probe_process_info.max_block_size);
        }
        else
        {
            /// probe_process_info.right_rows_to_be_added_when_matched == 0 mean not matched
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
    probe_process_info.updateEndRow<false>(current_row);
    return probe_process_info.cross_join_data->result_block_schema.cloneWithColumns(std::move(dst_columns));
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
std::pair<Block, bool> crossProbeBlockShallowCopyRightBlockAddNotMatchedRows(ProbeProcessInfo & probe_process_info)
{
    size_t num_existing_columns = probe_process_info.cross_join_data->left_column_index_in_left_block.size();
    MutableColumns dst_columns = probe_process_info.cross_join_data->result_block_schema.cloneEmptyColumns();
    if (probe_process_info.cross_join_data->row_num_filtered_by_left_condition > 0)
    {
        for (auto & dst_column : dst_columns)
            dst_column->reserve(probe_process_info.cross_join_data->row_num_filtered_by_left_condition);
    }
    auto * filter_ptr = probe_process_info.filter.get();
    auto * offset_ptr = probe_process_info.offsets_to_replicate.get();
    IColumn::Offset current_offset = 0;
    size_t num_columns_to_add = probe_process_info.cross_join_data->right_column_index_in_right_block.size();
    ColumnRawPtrs src_left_columns(num_existing_columns);
    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = probe_process_info.block
                                  .getByPosition(probe_process_info.cross_join_data->left_column_index_in_left_block[i])
                                  .column.get();
    }
    IColumn::Filter::value_type filter_column_value{};
    if constexpr (has_null_map)
    {
        // todo use column->filter(null_map) to construct the result block in batch
        for (size_t i = 0; i < probe_process_info.block.rows(); ++i)
        {
            if ((*probe_process_info.null_map)[i])
            {
                CrossJoinAdder<KIND, STRICTNESS>::addNotFound(
                    dst_columns,
                    num_existing_columns,
                    src_left_columns,
                    num_columns_to_add,
                    i,
                    filter_ptr,
                    current_offset,
                    offset_ptr,
                    probe_process_info.max_block_size);
                if (filter_ptr != nullptr)
                    filter_column_value = (*filter_ptr)[i];
            }
        }
    }
    /// construct fill filter and offset column
    if (!dst_columns[0]->empty())
    {
        assert(dst_columns[0]->size() == probe_process_info.cross_join_data->row_num_filtered_by_left_condition);
        if (filter_ptr != nullptr)
        {
            for (size_t i = 0; i < probe_process_info.cross_join_data->row_num_filtered_by_left_condition; ++i)
            {
                (*filter_ptr)[i] = filter_column_value;
            }
        }
        for (size_t i = 0; i < probe_process_info.cross_join_data->row_num_filtered_by_left_condition; ++i)
        {
            (*offset_ptr)[i] = i + 1;
        }
    }
    probe_process_info.all_rows_joined_finish = true;
    return {probe_process_info.cross_join_data->result_block_schema.cloneWithColumns(std::move(dst_columns)), false};
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
std::pair<Block, bool> crossProbeBlockShallowCopyRightBlockImpl(
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks)
{
    static_assert(KIND != ASTTableJoin::Kind::Cross_LeftOuterAnti);
    assert(probe_process_info.offsets_to_replicate != nullptr);

    size_t num_existing_columns = probe_process_info.cross_join_data->left_column_index_in_left_block.size();
    if constexpr (has_null_map)
    {
        /// skip filtered rows, the filtered rows will be handled at the end of this block
        while (probe_process_info.start_row < probe_process_info.block.rows()
               && (*probe_process_info.null_map)[probe_process_info.start_row])
        {
            ++probe_process_info.start_row;
        }
    }

    if (probe_process_info.start_row == probe_process_info.block.rows())
    {
        /// current probe block is done, collect un-matched rows
        return crossProbeBlockShallowCopyRightBlockAddNotMatchedRows<KIND, STRICTNESS, has_null_map>(
            probe_process_info);
    }
    assert(probe_process_info.cross_join_data->next_right_block_index < right_blocks.size());

    Block right_block = right_blocks[probe_process_info.cross_join_data->next_right_block_index];
    size_t right_row = right_block.rows();
    assert(right_row > 0);

    Block block = probe_process_info.cross_join_data->result_block_schema.cloneEmpty();
    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        /// left columns
        auto left_column_index = probe_process_info.cross_join_data->left_column_index_in_left_block[i];
        assert(probe_process_info.block.getByPosition(left_column_index).column != nullptr);
        Field value;
        probe_process_info.block.getByPosition(left_column_index).column->get(probe_process_info.start_row, value);
        block.getByPosition(i).column = block.getByPosition(i).type->createColumnConst(right_row, value);
    }
    auto right_column_num = probe_process_info.cross_join_data->right_column_index_in_right_block.size();
    if constexpr (KIND == ASTTableJoin::Kind::Cross_LeftOuterSemi)
    {
        --right_column_num;
    }
    for (size_t i = 0; i < right_column_num; ++i)
    {
        /// right columns
        block.getByPosition(i + num_existing_columns).column
            = right_block.getByPosition(probe_process_info.cross_join_data->right_column_index_in_right_block[i])
                  .column;
    }
    if constexpr (KIND == ASTTableJoin::Kind::Cross_LeftOuterSemi)
    {
        /// extra match_helper column for LeftOuterSemi join
        auto helper_index = block.columns() - 1;
        block.getByPosition(helper_index).column
            = block.getByPosition(helper_index).type->createColumnConst(right_row, FIELD_INT8_1);
    }
    /// construct filter and offset column
    if (probe_process_info.filter != nullptr)
    {
        (*probe_process_info.filter)[0] = 1;
    }
    (*probe_process_info.offsets_to_replicate)[0] = right_row;
    probe_process_info.cross_join_data->next_right_block_index++;
    probe_process_info.updateEndRow<true>(probe_process_info.start_row + 1);
    return {block, true};
}
} // namespace

Block crossProbeBlockDeepCopyRightBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks)
{
    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
#define DISPATCH(HAS_NULL_MAP)                                                                                         \
    if (kind == Cross && strictness == All)                                                                            \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross, All, HAS_NULL_MAP>(probe_process_info, right_blocks);      \
    else if (kind == Cross_LeftOuter && strictness == All)                                                             \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuter, All, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else if (kind == Cross_LeftOuter && strictness == Any)                                                             \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuter, Any, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else if (kind == Cross_Semi && strictness == All)                                                                  \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_Semi, All, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_Semi && strictness == Any)                                                                  \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_Semi, Any, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_Anti && strictness == All)                                                                  \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_Anti, All, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_Anti && strictness == Any)                                                                  \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_Anti, Any, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_LeftOuterSemi && strictness == All)                                                         \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(                          \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else if (kind == Cross_LeftOuterSemi && strictness == Any)                                                         \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(                          \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else if (kind == Cross_LeftOuterAnti && strictness == All)                                                         \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(                          \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else if (kind == Cross_LeftOuterAnti && strictness == Any)                                                         \
        return crossProbeBlockDeepCopyRightBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(                          \
            probe_process_info,                                                                                        \
            right_blocks);                                                                                             \
    else                                                                                                               \
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
}

std::pair<Block, bool> crossProbeBlockShallowCopyRightBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks)
{
    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
#define DISPATCH(HAS_NULL_MAP)                                                                                       \
    if (kind == Cross && strictness == All)                                                                          \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross, All, HAS_NULL_MAP>(probe_process_info, right_blocks); \
    else if (kind == Cross_LeftOuter && strictness == All)                                                           \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuter, All, HAS_NULL_MAP>(                         \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_LeftOuter && strictness == Any)                                                           \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuter, Any, HAS_NULL_MAP>(                         \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_Semi && strictness == All)                                                                \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_Semi, All, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_Semi && strictness == Any)                                                                \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_Semi, Any, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_Anti && strictness == All)                                                                \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_Anti, All, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_Anti && strictness == Any)                                                                \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_Anti, Any, HAS_NULL_MAP>(                              \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_LeftOuterSemi && strictness == All)                                                       \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(                     \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_LeftOuterSemi && strictness == Any)                                                       \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(                     \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_LeftOuterAnti && strictness == All)                                                       \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(                     \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else if (kind == Cross_LeftOuterAnti && strictness == Any)                                                       \
        return crossProbeBlockShallowCopyRightBlockImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(                     \
            probe_process_info,                                                                                      \
            right_blocks);                                                                                           \
    else                                                                                                             \
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
}

} // namespace DB
