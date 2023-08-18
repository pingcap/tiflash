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

#include <Columns/ColumnUtils.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>
#include <DataStreams/materializeBlock.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderMapEntry;

template <bool add_joined, typename Mapped>
struct AdderRowFlaggedMapEntry;

template <typename Mapped>
struct AdderMapEntry<ASTTableJoin::Strictness::Any, Mapped>
{
    static size_t add(
        const Mapped & mapped,
        size_t key_num,
        size_t num_columns_left,
        MutableColumns & columns_left,
        size_t num_columns_right,
        MutableColumns & columns_right,
        const void *&,
        const size_t)
    {
        for (size_t j = 0; j < num_columns_left; ++j)
            /// should fill the key column with key columns from right block
            /// but we don't care about the key column now so just insert a default value is ok.
            /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
            /// for detailed explanation
            columns_left[j]->insertDefault();

        for (size_t j = 0; j < num_columns_right; ++j)
            columns_right[j]->insertFrom(*mapped.block->getByPosition(key_num + j).column.get(), mapped.row_num);
        return 1;
    }
};

template <typename Mapped>
struct AdderMapEntry<ASTTableJoin::Strictness::All, Mapped>
{
    static size_t add(
        const Mapped & mapped,
        size_t key_num,
        size_t num_columns_left,
        MutableColumns & columns_left,
        size_t num_columns_right,
        MutableColumns & columns_right,
        const void *& next_element_in_row_list,
        const size_t max_row_added)
    {
        size_t rows_added = 0;
        auto current = &static_cast<const typename Mapped::Base_t &>(mapped);
        if unlikely (next_element_in_row_list != nullptr)
            current = reinterpret_cast<const typename Mapped::Base_t *>(next_element_in_row_list);
        for (; rows_added < max_row_added && current != nullptr; current = current->next)
        {
            /// handle left columns later to utilize insertManyDefaults
            for (size_t j = 0; j < num_columns_right; ++j)
                columns_right[j]->insertFrom(
                    *current->block->getByPosition(key_num + j).column.get(),
                    current->row_num);
            ++rows_added;
        }
        for (size_t j = 0; j < num_columns_left; ++j)
            /// should fill the key column with key columns from right block
            /// but we don't care about the key column now so just insert a default value is ok.
            /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
            /// for detailed explanation
            columns_left[j]->insertManyDefaults(rows_added);

        next_element_in_row_list = current;
        return rows_added;
    }
};

template <bool add_joined, typename Mapped>
struct AdderRowFlaggedMapEntry
{
    static size_t add(
        const Mapped & mapped,
        size_t key_num,
        size_t num_columns_left,
        MutableColumns & columns_left,
        size_t num_columns_right,
        MutableColumns & columns_right,
        const void *& next_element_in_row_list,
        const size_t max_row_added)
    {
        size_t rows_added = 0;
        const auto * current = &static_cast<const typename Mapped::Base_t &>(mapped);
        if unlikely (next_element_in_row_list != nullptr)
            current = reinterpret_cast<const typename Mapped::Base_t *>(next_element_in_row_list);
        for (; rows_added < max_row_added && current != nullptr; current = current->next)
        {
            bool flag = current->getUsed();
            if constexpr (!add_joined)
                flag = !flag;
            if (flag)
            {
                /// handle left columns later to utilize insertManyDefaults if any
                for (size_t j = 0; j < num_columns_right; ++j)
                    columns_right[j]->insertFrom(
                        *current->block->getByPosition(key_num + j).column.get(),
                        current->row_num);
                ++rows_added;
            }
        }
        for (size_t j = 0; j < num_columns_left; ++j)
            /// should fill the key column with key columns from right block
            /// but we don't care about the key column now so just insert a default value is ok.
            /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
            /// for detailed explanation
            columns_left[j]->insertManyDefaults(rows_added);

        next_element_in_row_list = current;
        return rows_added;
    }
};

ScanHashMapAfterProbeBlockInputStream::ScanHashMapAfterProbeBlockInputStream(
    const Join & parent_,
    const Block & left_sample_block,
    size_t index_,
    size_t step_,
    size_t max_block_size_)
    : parent(parent_)
    , index(index_)
    , step(step_)
    , max_block_size(max_block_size_)
{
    size_t build_concurrency = parent.getBuildConcurrency();
    if (unlikely(step > build_concurrency || index >= build_concurrency))
        LOG_WARNING(
            parent.log,
            "The concurrency of ScanHashMapAfterProbBlockInputStream is larger than join build concurrency");

    /** left_sample_block contains keys and "left" columns.
          * result_sample_block - keys, "left" columns, and "right" columns.
          */

    size_t num_columns_left = left_sample_block.columns();
    if (isRightSemiFamily(parent.getKind()))
        num_columns_left = 0;
    else
        result_sample_block = materializeBlock(left_sample_block);

    size_t num_columns_right = parent.sample_block_with_columns_to_add.columns();
    /// Add columns from the right-side table to the block.
    for (size_t i = 0; i < num_columns_right; ++i)
    {
        const ColumnWithTypeAndName & src_column = parent.sample_block_with_columns_to_add.getByPosition(i);
        result_sample_block.insert(src_column.cloneEmpty());
    }

    column_indices_left.reserve(num_columns_left);
    column_indices_right.reserve(num_columns_right);

    for (size_t i = 0; i < num_columns_left; ++i)
    {
        column_indices_left.push_back(i);
    }

    for (size_t i = 0; i < num_columns_right; ++i)
        column_indices_right.push_back(num_columns_left + i);

    for (size_t i = 0; i < num_columns_left; ++i)
    {
        const auto & column_with_type_and_name = result_sample_block.getByPosition(column_indices_left[i]);
        if (parent.key_names_left.end()
            == std::find(parent.key_names_left.begin(), parent.key_names_left.end(), column_with_type_and_name.name))
            /// if it is not the key, then convert to nullable, if it is key, then just keep the original type
            /// actually we don't care about the key column now refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
            /// for detailed explanation
            convertColumnToNullable(result_sample_block.getByPosition(column_indices_left[i]));
    }

    columns_left.resize(num_columns_left);
    columns_right.resize(num_columns_right);
    current_partition_index = index;

    for (const auto & name : parent.tidb_output_column_names)
    {
        auto & column = result_sample_block.getByName(name);
        projected_sample_block.insert(column);
    }
}

Block ScanHashMapAfterProbeBlockInputStream::readImpl()
{
    /// If build concurrency is less than non join concurrency,
    /// just return empty block for extra non joined block input stream read
    if (unlikely(index >= parent.getBuildConcurrency()))
        return {};
    if unlikely (parent.active_build_threads != 0 || parent.active_probe_threads != 0)
    {
        /// build/probe is not finished yet, the query must be cancelled, so just return {}
        LOG_WARNING(
            parent.log,
            "ScanHashMapAfterProbe read without non zero active_build_threads/active_probe_threads, return empty "
            "block");
        return {};
    }
    if (!parent.has_build_data_in_memory)
        /// no build data in memory, the scan hash map result must be empty
        return {};

    size_t num_columns_left = column_indices_left.size();
    size_t num_columns_right = column_indices_right.size();
    IColumn * row_counter_column = nullptr;

    for (size_t i = 0; i < num_columns_left; ++i)
    {
        const auto & src_col = result_sample_block.safeGetByPosition(column_indices_left[i]);
        columns_left[i] = src_col.type->createColumn();
        if (row_counter_column == nullptr)
            row_counter_column = columns_left[i].get();
    }

    for (size_t i = 0; i < num_columns_right; ++i)
    {
        const auto & src_col = result_sample_block.safeGetByPosition(column_indices_right[i]);
        columns_right[i] = src_col.type->createColumn();
        if (row_counter_column == nullptr)
            row_counter_column = columns_right[i].get();
    }
    assert(row_counter_column != nullptr);

    while (current_partition_index < parent.getBuildConcurrency() && row_counter_column->size() < max_block_size)
    {
        switch (parent.kind)
        {
        case ASTTableJoin::Kind::RightSemi:
            if (parent.has_other_condition)
                fillColumnsUsingCurrentPartition<true, true>(
                    num_columns_left,
                    columns_left,
                    num_columns_right,
                    columns_right,
                    row_counter_column);
            else
                fillColumnsUsingCurrentPartition<false, true>(
                    num_columns_left,
                    columns_left,
                    num_columns_right,
                    columns_right,
                    row_counter_column);
            break;
        case ASTTableJoin::Kind::RightAnti:
        case ASTTableJoin::Kind::RightOuter:
            if (parent.has_other_condition)
                fillColumnsUsingCurrentPartition<true, false>(
                    num_columns_left,
                    columns_left,
                    num_columns_right,
                    columns_right,
                    row_counter_column);
            else
                fillColumnsUsingCurrentPartition<false, false>(
                    num_columns_left,
                    columns_left,
                    num_columns_right,
                    columns_right,
                    row_counter_column);
            break;
        default:
            fillColumnsUsingCurrentPartition<false, false>(
                num_columns_left,
                columns_left,
                num_columns_right,
                columns_right,
                row_counter_column);
        }
    }

    if (row_counter_column->empty())
        return {};

    Block res = result_sample_block.cloneEmpty();
    for (size_t i = 0; i < num_columns_left; ++i)
        res.getByPosition(column_indices_left[i]).column = std::move(columns_left[i]);
    for (size_t i = 0; i < num_columns_right; ++i)
        res.getByPosition(column_indices_right[i]).column = std::move(columns_right[i]);

    /// remove useless columns
    Block projected_block;
    for (const auto & name : parent.tidb_output_column_names)
    {
        auto & column = res.getByName(name);
        projected_block.insert(std::move(column));
    }
    return projected_block;
}

template <bool row_flagged, bool output_joined_rows>
void ScanHashMapAfterProbeBlockInputStream::fillColumnsUsingCurrentPartition(
    size_t num_columns_left,
    MutableColumns & mutable_columns_left,
    size_t num_columns_right,
    MutableColumns & mutable_columns_right,
    IColumn * row_counter_column)
{
    const auto & partition = parent.partitions[current_partition_index];
    if (parent.isSpilled() && partition->isSpill())
    {
        /// if the partition is spilled, just skip it
        advancedToNextPartition();
        return;
    }
    if constexpr (!output_joined_rows)
    {
        if (!not_mapped_row_pos_inited)
        {
            not_mapped_row_pos = partition->getRowsNotInsertedToMap()->head.next;
            not_mapped_row_pos_inited = true;
        }
    }
    if constexpr (row_flagged)
    {
        assert(parent.strictness == ASTTableJoin::Strictness::All);
        switch (parent.join_map_method)
        {
#define M(METHOD)                                                             \
    case JoinMapMethod::METHOD:                                               \
        fillColumns<ASTTableJoin::Strictness::All, true, output_joined_rows>( \
            *partition->maps_all_full_with_row_flag.METHOD,                   \
            num_columns_left,                                                 \
            mutable_columns_left,                                             \
            num_columns_right,                                                \
            mutable_columns_right,                                            \
            row_counter_column);                                              \
        break;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }
    }
    else if (parent.strictness == ASTTableJoin::Strictness::Any)
    {
        assert(!output_joined_rows);
        switch (parent.join_map_method)
        {
#define M(METHOD)                                                 \
    case JoinMapMethod::METHOD:                                   \
        fillColumns<ASTTableJoin::Strictness::Any, false, false>( \
            *partition->maps_any_full.METHOD,                     \
            num_columns_left,                                     \
            mutable_columns_left,                                 \
            num_columns_right,                                    \
            mutable_columns_right,                                \
            row_counter_column);                                  \
        break;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }
    }
    else if (parent.strictness == ASTTableJoin::Strictness::All)
    {
        switch (parent.join_map_method)
        {
#define M(METHOD)                                                              \
    case JoinMapMethod::METHOD:                                                \
        fillColumns<ASTTableJoin::Strictness::All, false, output_joined_rows>( \
            *partition->maps_all_full.METHOD,                                  \
            num_columns_left,                                                  \
            mutable_columns_left,                                              \
            num_columns_right,                                                 \
            mutable_columns_right,                                             \
            row_counter_column);                                               \
        break;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }
    }
    else
        throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
}

struct RowCountInfo
{
    RowCountInfo(size_t current_rows_, size_t max_rows_)
        : current_rows(current_rows_)
        , added_rows(0)
        , max_rows(max_rows_)
    {}
    inline size_t getAddedRows() const { return added_rows; }
    inline size_t getCurrentRows() const { return current_rows; }
    inline bool reachMaxRows() const { return current_rows == max_rows; }
    inline void inc(size_t rows)
    {
        added_rows += rows;
        current_rows += rows;
    }
    inline size_t availableRowCount() const { return max_rows - current_rows; }

private:
    size_t current_rows;
    size_t added_rows;
    size_t max_rows;
};

template <ASTTableJoin::Strictness STRICTNESS, bool row_flagged, bool output_joined_rows, typename Map>
void ScanHashMapAfterProbeBlockInputStream::fillColumns(
    const Map & map,
    size_t num_columns_left,
    MutableColumns & mutable_columns_left,
    size_t num_columns_right,
    MutableColumns & mutable_columns_right,
    IColumn * row_counter_column)
{
    size_t key_num = parent.key_names_right.size();
    /// first add rows that is not in the hash table
    RowCountInfo row_count_info(row_counter_column->size(), max_block_size);
    while (!output_joined_rows && not_mapped_row_pos != nullptr)
    {
        row_count_info.inc(1);
        /// handle left columns later to utilize insertManyDefaults
        for (size_t j = 0; j < num_columns_right; ++j)
            mutable_columns_right[j]->insertFrom(
                *not_mapped_row_pos->block->getByPosition(key_num + j).column.get(),
                not_mapped_row_pos->row_num);

        not_mapped_row_pos = not_mapped_row_pos->next;
        if (row_count_info.reachMaxRows())
            break;
    }
    /// Fill left columns with defaults
    for (size_t j = 0; j < num_columns_left; ++j)
        /// should fill the key column with key columns from right block
        /// but we don't care about the key column now so just insert a default value is ok.
        /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
        /// for detailed explanation
        mutable_columns_left[j]->insertManyDefaults(row_count_info.getAddedRows());

    if (row_count_info.reachMaxRows())
        return;

    /// then add rows that in hash table, but not joined
    if (!pos_in_hashmap_inited)
    {
        pos_in_hashmap = decltype(pos_in_hashmap)(
            static_cast<void *>(new typename Map::const_iterator(map.begin())),
            [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });
        pos_in_hashmap_inited = true;
    }

    /// use pointer instead of reference because `it` need to be re-assigned latter
    auto it = reinterpret_cast<typename Map::const_iterator *>(pos_in_hashmap.get());
    auto end = map.end();

    for (; *it != end;)
    {
        if constexpr (row_flagged)
            row_count_info.inc(AdderRowFlaggedMapEntry<output_joined_rows, typename Map::mapped_type>::add(
                (*it)->getMapped(),
                key_num,
                num_columns_left,
                mutable_columns_left,
                num_columns_right,
                mutable_columns_right,
                next_element_in_row_list,
                row_count_info.availableRowCount()));
        else
        {
            bool should_skip = (*it)->getMapped().getUsed();
            if constexpr (output_joined_rows)
                should_skip = !should_skip;
            if (should_skip)
            {
                ++(*it);
                continue;
            }

            row_count_info.inc(AdderMapEntry<STRICTNESS, typename Map::mapped_type>::add(
                (*it)->getMapped(),
                key_num,
                num_columns_left,
                mutable_columns_left,
                num_columns_right,
                mutable_columns_right,
                next_element_in_row_list,
                row_count_info.availableRowCount()));
        }
        assert(row_count_info.getCurrentRows() <= max_block_size);

        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
        {
            ++(*it);
        }
        else if (next_element_in_row_list == nullptr)
        {
            /// next_element_in_row_list == nullptr means current row_list is done, so move the iterator
            ++(*it);
        }

        if (row_count_info.reachMaxRows())
            break;
    }

    if (*it == end)
        advancedToNextPartition();
}
} // namespace DB
