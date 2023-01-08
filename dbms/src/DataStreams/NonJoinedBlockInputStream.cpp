// Copyright 2022 PingCAP, Ltd.
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

#include <DataStreams/NonJoinedBlockInputStream.h>
#include <DataStreams/materializeBlock.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Any, Mapped>
{
    static size_t add(const Mapped & mapped, size_t key_num, size_t num_columns_left, MutableColumns & columns_left, size_t num_columns_right, MutableColumns & columns_right)
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
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
    static size_t add(const Mapped & mapped, size_t key_num, size_t num_columns_left, MutableColumns & columns_left, size_t num_columns_right, MutableColumns & columns_right)
    {
        size_t rows_added = 0;
        for (auto current = &static_cast<const typename Mapped::Base_t &>(mapped); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < num_columns_left; ++j)
                /// should fill the key column with key columns from right block
                /// but we don't care about the key column now so just insert a default value is ok.
                /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
                /// for detailed explanation
                columns_left[j]->insertDefault();

            for (size_t j = 0; j < num_columns_right; ++j)
                columns_right[j]->insertFrom(*current->block->getByPosition(key_num + j).column.get(), current->row_num);
            rows_added++;
        }
        return rows_added;
    }
};

NonJoinedBlockInputStream::NonJoinedBlockInputStream(const Join & parent_, const Block & left_sample_block, size_t index_, size_t step_, size_t max_block_size_)
    : parent(parent_)
    , index(index_)
    , step(step_)
    , max_block_size(max_block_size_)
    , add_not_mapped_rows(true)
{
    size_t build_concurrency = parent.getBuildConcurrency();
    if (unlikely(step > build_concurrency || index >= build_concurrency))
        LOG_WARNING(parent.log, "The concurrency of NonJoinedBlockInputStream is larger than join build concurrency");

    /** left_sample_block contains keys and "left" columns.
          * result_sample_block - keys, "left" columns, and "right" columns.
          */

    size_t num_columns_left = left_sample_block.columns();
    size_t num_columns_right = parent.sample_block_with_columns_to_add.columns();

    result_sample_block = materializeBlock(left_sample_block);

    /// Add columns from the right-side table to the block.
    for (size_t i = 0; i < num_columns_right; ++i)
    {
        const ColumnWithTypeAndName & src_column = parent.sample_block_with_columns_to_add.getByPosition(i);
        result_sample_block.insert(src_column.cloneEmpty());
    }

    column_indices_left.reserve(num_columns_left);
    column_indices_right.reserve(num_columns_right);
    BoolVec is_key_column_in_left_block(num_columns_left, false);

    for (size_t i = 0; i < num_columns_left; ++i)
    {
        column_indices_left.push_back(i);
    }

    for (size_t i = 0; i < num_columns_right; ++i)
        column_indices_right.push_back(num_columns_left + i);

    /// If use_nulls, convert left columns to Nullable.
    if (parent.use_nulls)
    {
        for (size_t i = 0; i < num_columns_left; ++i)
        {
            const auto & column_with_type_and_name = result_sample_block.getByPosition(column_indices_left[i]);
            if (parent.key_names_left.end() == std::find(parent.key_names_left.begin(), parent.key_names_left.end(), column_with_type_and_name.name))
                /// if it is not the key, then convert to nullable, if it is key, then just keep the original type
                /// actually we don't care about the key column now refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
                /// for detailed explanation
                convertColumnToNullable(result_sample_block.getByPosition(column_indices_left[i]));
        }
    }

    columns_left.resize(num_columns_left);
    columns_right.resize(num_columns_right);
    next_index = index;
}

Block NonJoinedBlockInputStream::readImpl()
{
    /// build concurrency is less than non join concurrency,
    /// just return empty block for extra non joined block input stream read
    if (index >= parent.getBuildConcurrency())
        return Block();
    if (parent.blocks.empty())
        return Block();

    if (add_not_mapped_rows)
    {
        setNextCurrentNotMappedRow();
        add_not_mapped_rows = false;
    }

    if (parent.strictness == ASTTableJoin::Strictness::Any)
        return createBlock<ASTTableJoin::Strictness::Any>(parent.maps_any_full);
    else if (parent.strictness == ASTTableJoin::Strictness::All)
        return createBlock<ASTTableJoin::Strictness::All>(parent.maps_all_full);
    else
        throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
}

void NonJoinedBlockInputStream::setNextCurrentNotMappedRow()
{
    while (current_not_mapped_row == nullptr && next_index < parent.rows_not_inserted_to_map.size())
    {
        current_not_mapped_row = parent.rows_not_inserted_to_map[next_index]->next;
        next_index += step;
    }
}

template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
Block NonJoinedBlockInputStream::createBlock(const Maps & maps)
{
    size_t num_columns_left = column_indices_left.size();
    size_t num_columns_right = column_indices_right.size();

    for (size_t i = 0; i < num_columns_left; ++i)
    {
        const auto & src_col = result_sample_block.safeGetByPosition(column_indices_left[i]);
        columns_left[i] = src_col.type->createColumn();
    }

    for (size_t i = 0; i < num_columns_right; ++i)
    {
        const auto & src_col = result_sample_block.safeGetByPosition(column_indices_right[i]);
        columns_right[i] = src_col.type->createColumn();
    }

    size_t rows_added = 0;

    switch (parent.type)
    {
#define M(TYPE)                                                                                                             \
    case Join::Type::TYPE:                                                                                                  \
        rows_added = fillColumns<STRICTNESS>(*maps.TYPE, num_columns_left, columns_left, num_columns_right, columns_right); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }

    if (!rows_added)
        return {};

    Block res = result_sample_block.cloneEmpty();
    for (size_t i = 0; i < num_columns_left; ++i)
        res.getByPosition(column_indices_left[i]).column = std::move(columns_left[i]);
    for (size_t i = 0; i < num_columns_right; ++i)
        res.getByPosition(column_indices_right[i]).column = std::move(columns_right[i]);

    return res;
}


template <ASTTableJoin::Strictness STRICTNESS, typename Map>
size_t NonJoinedBlockInputStream::fillColumns(const Map & map,
                                              size_t num_columns_left,
                                              MutableColumns & mutable_columns_left,
                                              size_t num_columns_right,
                                              MutableColumns & mutable_columns_right)
{
    size_t rows_added = 0;
    size_t key_num = parent.key_names_right.size();
    /// first add rows that is not in the hash table
    while (current_not_mapped_row != nullptr)
    {
        rows_added++;
        for (size_t j = 0; j < num_columns_left; ++j)
            /// should fill the key column with key columns from right block
            /// but we don't care about the key column now so just insert a default value is ok.
            /// refer to https://github.com/pingcap/tiflash/blob/v6.5.0/dbms/src/Flash/Coprocessor/DAGExpressionAnalyzer.cpp#L953
            /// for detailed explanation
            mutable_columns_left[j]->insertDefault();

        for (size_t j = 0; j < num_columns_right; ++j)
            mutable_columns_right[j]->insertFrom(*current_not_mapped_row->block->getByPosition(key_num + j).column.get(),
                                                 current_not_mapped_row->row_num);

        current_not_mapped_row = current_not_mapped_row->next;
        setNextCurrentNotMappedRow();
        if (rows_added == max_block_size)
        {
            return rows_added;
        }
    }

    /// then add rows that in hash table, but not joined
    if (!position)
    {
        current_segment = index;
        position = decltype(position)(
            static_cast<void *>(new typename Map::SegmentType::HashTable::const_iterator(map.getSegmentTable(current_segment).begin())),
            [](void * ptr) { delete reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(ptr); });
    }

    /// use pointer instead of reference because `it` need to be re-assigned latter
    auto it = reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(position.get());
    auto end = map.getSegmentTable(current_segment).end();

    for (; *it != end || current_segment + step < map.getSegmentSize(); ++(*it))
    {
        if (*it == end)
        {
            // move to next internal hash table
            do
            {
                current_segment += step;
                position = decltype(position)(
                    static_cast<void *>(new typename Map::SegmentType::HashTable::const_iterator(
                        map.getSegmentTable(current_segment).begin())),
                    [](void * ptr) { delete reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(ptr); });
                it = reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(position.get());
                end = map.getSegmentTable(current_segment).end();
            } while (*it == end && current_segment < map.getSegmentSize() - step);
            if (*it == end)
                break;
        }
        if ((*it)->getMapped().getUsed())
            continue;

        rows_added += AdderNonJoined<STRICTNESS, typename Map::mapped_type>::add((*it)->getMapped(), key_num, num_columns_left, mutable_columns_left, num_columns_right, mutable_columns_right);

        if (rows_added >= max_block_size)
        {
            ++(*it);
            break;
        }
    }
    return rows_added;
}
} // namespace DB
