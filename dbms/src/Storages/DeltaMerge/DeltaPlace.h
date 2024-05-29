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

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <common/logger_useful.h>

#include <type_traits>


namespace DB
{
namespace DM
{
inline int compareTuple(const Columns & left, size_t l_pos, const Columns & right, size_t r_pos, const SortDescription & sort_desc)
{
    auto num_sort_columns = sort_desc.size();
    for (size_t i = 0; i < num_sort_columns; ++i)
    {
        int direction = sort_desc[i].direction;
        int nulls_direction = sort_desc[i].nulls_direction;
        int res = direction * left[i]->compareAt(l_pos, r_pos, *(right[i]), nulls_direction);
        if (res != 0)
            return res;
    }
    return 0;
}

struct RidGenerator
{
    SkippableBlockInputStreamPtr stable_stream;
    const SortDescription & sort_desc;
    const size_t num_sort_columns;

    Columns delta_block_columns;
    size_t delta_block_offset;
    size_t delta_block_pos;
    // Whether this row's pk duplicates with the next one, if so, they share the same rid.
    std::vector<UInt8> delta_block_dup_next;
    // Whether current row's pk duplicates with the previous one. Used by Upsert.
    bool dup_prev = false;

    Columns stable_block_columns;
    size_t stable_block_rows = 0;
    size_t stable_block_pos = 0;
    bool stable_finished = false;

    UInt64 rid = 0;

    RidGenerator(const SkippableBlockInputStreamPtr & stable_stream_,
                 const SortDescription & sort_desc_,
                 const Block & delta_block,
                 size_t offset,
                 size_t limit)
        : stable_stream(stable_stream_)
        , sort_desc(sort_desc_)
        , num_sort_columns(sort_desc.size())
        , delta_block_offset(offset)
        , delta_block_pos(0)
        , delta_block_dup_next(limit, 0)
    {
        stable_stream->readPrefix();

        size_t skip = 0;
        stable_stream->getSkippedRows(skip);
        rid = skip;

        for (size_t i = 0; i < num_sort_columns; ++i)
            delta_block_columns.push_back(delta_block.getByName(sort_desc[i].column_name).column);

        // Check continually tuples with identical primary key.
        for (size_t i = 0; i < limit - 1; ++i)
        {
            auto res = compareTuple(delta_block_columns, offset + i, delta_block_columns, offset + i + 1, sort_desc);
            if (unlikely(res > 0))
                throw Exception("Illegal delta data, the next row is expected larger than the previous row");
            delta_block_dup_next[i] = (res == 0);
        }
    }

    ~RidGenerator() { stable_stream->readSuffix(); }

    inline int compareModifyToStable() const
    {
        return compareTuple(delta_block_columns, delta_block_offset + delta_block_pos, stable_block_columns, stable_block_pos, sort_desc);
    }

    inline bool fillStableBlockIfNeed()
    {
        if (stable_finished)
            return false;
        if (!stable_block_columns.empty() && stable_block_pos < stable_block_rows)
            return true;
        stable_block_columns.clear();
        stable_block_rows = 0;
        stable_block_pos = 0;
        auto block = stable_stream->read();
        if (!block)
        {
            stable_finished = true;
            return false;
        }
        stable_block_rows = block.rows();
        for (size_t column_id = 0; column_id < num_sort_columns; ++column_id)
            stable_block_columns.push_back(block.getByName(sort_desc[column_id].column_name).column);

#ifndef NDEBUG
        for (size_t row = 0; row < stable_block_rows - 1; ++row)
        {
            auto res = compareTuple(stable_block_columns, row, stable_block_columns, row + 1, sort_desc);
            if (unlikely(res >= 0))
                throw Exception("Illegal stable data, the next row@" + DB::toString(row + 1) + " is expected larger than the previous row@"
                                + DB::toString(row));
        }
#endif
        return true;
    }

    std::pair<UInt64, bool> nextForUpsert()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res > 0)
            {
                ++stable_block_pos;
                ++rid;
                continue;
            }

            auto cur_dup_prev = dup_prev;
            dup_prev = delta_block_dup_next[delta_block_pos++];
            if (res == 0)
            {
                if (dup_prev)
                    return {rid, true};
                else
                {
                    ++stable_block_pos;
                    return {rid++, true};
                }
            }
            else if (res < 0)
            {
                if (dup_prev)
                    return {rid, cur_dup_prev};
                else
                    return {rid++, cur_dup_prev};
            }
        }

        auto cur_dup_prev = dup_prev;
        dup_prev = delta_block_dup_next[delta_block_pos];
        ++delta_block_pos;
        if (dup_prev)
            return {rid, cur_dup_prev};
        else
            return {rid++, cur_dup_prev};
    }


    Int64 nextForDelete()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res == 0)
            {
                ++delta_block_pos;
                ++stable_block_pos;
                return rid;
            }
            else if (res > 0)
            {
                ++stable_block_pos;
                ++rid;
            }
            else // res < 0
            {
                // We support idempotent. None existing delete rows are ignored.
                ++delta_block_pos;
                ++rid;
                return -1;
            }
        }
        throw Exception("Rows not found");
    }
};

/**
 * Index the block which is already sorted by primary keys. The indexing result is recorded into delta_tree.
 * Returns fully index or not (Some rows not match range won't be indexed).
 */
template <bool use_row_id_ref, class DeltaTree>
bool placeInsert(const SkippableBlockInputStreamPtr & stable, //
                 const Block & delta_block,
                 const RowKeyRange & range,
                 bool relevant_place,
                 DeltaTree & delta_tree,
                 size_t delta_value_space_offset,
                 const IColumn::Permutation & row_id_ref,
                 const SortDescription & sort)
{
    auto rows = delta_block.rows();

    size_t offset = 0;
    size_t limit = rows;

    // Only filter out irrelevant rows if relevant_place is true. Otherwise, range should always be ALL.
    if (relevant_place)
    {
        std::tie(offset, limit) = RowKeyFilter::getPosRangeOfSorted(range, delta_block.getByPosition(0).column, 0, rows);
        if (!limit)
            return rows == limit;
    }

    RidGenerator rid_gen(stable, sort, delta_block, offset, limit);

    using Rids = std::vector<std::pair<UInt64, bool>>;
    Rids rids(limit);
    for (size_t i = 0; i < limit; ++i)
        rids[i] = rid_gen.nextForUpsert();

    for (size_t i = 0; i < limit; ++i)
    {
        auto [rid, dup] = rids[i];
        UInt64 tuple_id;
        if constexpr (use_row_id_ref)
            tuple_id = delta_value_space_offset + row_id_ref[offset + i];
        else
            tuple_id = delta_value_space_offset + (offset + i);

        if (dup)
        {
            delta_tree.addDelete(rid);
            delta_tree.setMaxDupTupleID(tuple_id);
        }
        delta_tree.addInsert(rid, tuple_id);
    }

    return rows == limit;
}

/// Returns fully index or not (Some rows not match range won't be indexed).
template <class DeltaTree>
bool placeDelete(const SkippableBlockInputStreamPtr & stable, //
                 const Block & delta_block,
                 const RowKeyRange & range,
                 bool relevant_place,
                 DeltaTree & delta_tree,
                 const SortDescription & sort)
{
    auto rows = delta_block.rows();

    size_t offset = 0;
    size_t limit = rows;

    // Only filter out irrelevant rows if relevant_place is true. Otherwise, range should always be ALL.
    if (relevant_place)
    {
        std::tie(offset, limit) = RowKeyFilter::getPosRangeOfSorted(range, delta_block.getByPosition(0).column, 0, rows);
        if (!limit)
            return rows == limit;
    }

    RidGenerator rid_gen(stable, sort, delta_block, offset, limit);

    using Rids = std::vector<Int64>;
    Rids rids(limit);
    for (size_t i = 0; i < limit; ++i)
        rids[i] = rid_gen.nextForDelete();
    for (size_t i = 0; i < limit; ++i)
    {
        if (rids[i] >= 0)
            delta_tree.addDelete(rids[i]);
    }
    return rows == limit;
}

} // namespace DM
} // namespace DB