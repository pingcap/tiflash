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

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB
{
namespace DM
{
/// Reorganize the boundary of blocks. The rows with the same primary key(s) will be squashed
/// into the same output block. The output blocks are sorted by increasing pk && version.
/// Note that the `child` must be a sorted input stream with increasing pk && version. If you are
/// not sure the child stream is sorted with increasing pk && version, set `need_extra_sort` to
/// be `true`.
template <bool need_extra_sort>
class PKSquashingBlockInputStream final : public IBlockInputStream
{
public:
    PKSquashingBlockInputStream(
        BlockInputStreamPtr child,
        ColId pk_column_id_,
        bool is_common_handle_,
        size_t split_id_ = DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT)
        : sorted_input_stream(child)
        , pk_column_id(pk_column_id_)
        , is_common_handle(is_common_handle_)
        , split_id(split_id_)
    {
        assert(sorted_input_stream != nullptr);
        cur_block = {};
        children.push_back(child);
    }

    String getName() const override { return "PKSquashing"; }
    Block getHeader() const override { return sorted_input_stream->getHeader(); }

    void readPrefix() override
    {
        forEachChild([](IBlockInputStream & child) {
            child.readPrefix();
            return false;
        });
    }

    void readSuffix() override
    {
        forEachChild([](IBlockInputStream & child) {
            child.readSuffix();
            return false;
        });
    }

    Block read() override
    {
        if (first_read)
        {
            next_block = DB::DM::readNextBlock(sorted_input_stream);
            first_read = false;
        }

        cur_block = next_block;
        if (!cur_block)
            return finializeBlock(std::move(cur_block));

        while (true)
        {
            next_block = DB::DM::readNextBlock(sorted_input_stream);

#ifndef NDEBUG
            if (next_block && !isSameSchema(cur_block, next_block))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "schema not match! cur_block={} next_block={}",
                    cur_block.dumpJsonStructure(),
                    next_block.dumpJsonStructure());
            }
#endif

            const size_t cut_offset
                = findCutOffsetInNextBlock(split_id, cur_block, next_block, pk_column_id, is_common_handle);
            if (unlikely(cut_offset == 0))
                // There is no pk overlap between `cur_block` and `next_block`, or `next_block` is empty, just return `cur_block`.
                return finializeBlock(std::move(cur_block));
            else
            {
                const size_t next_block_nrows = next_block.rows();
                for (size_t col_idx = 0; col_idx != cur_block.columns(); ++col_idx)
                {
                    auto & cur_col_with_name = cur_block.getByPosition(col_idx);
                    auto & next_col_with_name = next_block.getByPosition(col_idx);
                    auto * cur_col_raw = const_cast<IColumn *>(cur_col_with_name.column.get());
                    cur_col_raw->insertRangeFrom(*next_col_with_name.column, 0, cut_offset);

                    if (cut_offset != next_block_nrows)
                    {
                        // TODO: we can track the valid range instead of copying data.
                        size_t nrows_to_copy = next_block_nrows - cut_offset;
                        // Pop front `cut_offset` elems from `next_col_with_name`
                        assert(next_block_nrows == next_col_with_name.column->size());
                        MutableColumnPtr cutted_next_column = next_col_with_name.column->cloneEmpty();
                        cutted_next_column->insertRangeFrom(*next_col_with_name.column, cut_offset, nrows_to_copy);
                        next_col_with_name.column = cutted_next_column->getPtr();
                    }
                }
                if (cut_offset != next_block_nrows)
                {
                    // We merge some rows to `cur_block`, return it.
                    return finializeBlock(std::move(cur_block));
                }
                // else we merge all rows from `next_block` to `cur_block`, continue to check if we should merge more blocks.
            }
        }
    }

private:
    static size_t findCutOffsetInNextBlock(
        size_t split_id,
        const Block & cur_block,
        const Block & next_block,
        const ColId pk_column_id,
        bool is_common_handle)
    {
        assert(cur_block);
        if (!next_block)
            return 0;

        auto cur_col = getByColumnId(cur_block, pk_column_id).column;
        RowKeyColumnContainer cur_rowkey_column(cur_col, is_common_handle);
        const auto last_curr_pk = cur_rowkey_column.getRowKeyValue(cur_col->size() - 1);
        auto next_col = getByColumnId(next_block, pk_column_id).column;
        RowKeyColumnContainer next_rowkey_column(next_col, is_common_handle);
        size_t cut_offset = 0;
        for (/* */; cut_offset < next_col->size(); ++cut_offset)
        {
            const auto next_pk = next_rowkey_column.getRowKeyValue(cut_offset);
            if (next_pk != last_curr_pk)
            {
                if constexpr (DM_RUN_CHECK)
                {
                    if (unlikely(next_pk < last_curr_pk))
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "InputStream is not sorted, pk in next block {} is smaller than current block {}, "
                            "split_id={}",
                            next_pk.toDebugString(),
                            last_curr_pk.toDebugString(),
                            split_id);
                }
                break;
            }
        }
        return cut_offset;
    }

    static Block finializeBlock(Block && block)
    {
        if constexpr (need_extra_sort)
        {
            // Sort by handle & version in ascending order.
            static SortDescription sort{
                SortColumnDescription{MutSup::extra_handle_column_name, 1, 0},
                SortColumnDescription{MutSup::version_column_name, 1, 0}};
            if (block.rows() > 1 && !isAlreadySorted(block, sort))
                stableSortBlock(block, sort);
        }
        return std::move(block);
    }

private:
    BlockInputStreamPtr sorted_input_stream;
    const ColId pk_column_id;

    Block cur_block;
    Block next_block;

    bool first_read = true;
    const bool is_common_handle;
    // Setting to non `HEAD_OR_ONLY_SPLIT` means this is a part stream.
    const size_t split_id;
};

} // namespace DM
} // namespace DB
