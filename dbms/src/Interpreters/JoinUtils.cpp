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

#include <Columns/ColumnUtils.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullableUtils.h>

namespace DB
{
void ProbeProcessInfo::resetBlock(Block && block_, size_t partition_index_)
{
    block = std::move(block_);
    partition_index = partition_index_;
    start_row = 0;
    end_row = 0;
    all_rows_joined_finish = false;
    // If the probe block size is greater than max_block_size, we will set max_block_size to the probe block size to avoid some unnecessary split.
    max_block_size = std::max(max_block_size, block.rows());
    // min_result_block_size is used to avoid generating too many small block, use 50% of the block size as the default value
    min_result_block_size = std::max(1, (std::min(block.rows(), max_block_size) + 1) / 2);
    prepare_for_probe_done = false;
    null_map = nullptr;
    null_map_holder = nullptr;
    key_columns.clear();
    materialized_columns.clear();
    filter.reset();
    offsets_to_replicate.reset();
}

void ProbeProcessInfo::updateStartRow()
{
    assert(start_row <= end_row);
    start_row = end_row;
    if (filter != nullptr)
        filter->resize(block.rows());
    if (offsets_to_replicate != nullptr)
        offsets_to_replicate->resize(block.rows());
}

ColumnRawPtrs extractAndMaterializeKeyColumns(const Block & block, Columns & materialized_columns, const Strings & key_columns_names)
{
    ColumnRawPtrs key_columns(key_columns_names.size());
    for (size_t i = 0; i < key_columns_names.size(); ++i)
    {
        key_columns[i] = block.getByName(key_columns_names[i]).column.get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }
    return key_columns;
}

void recordFilteredRows(const Block & block, const String & filter_column, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map)
{
    if (filter_column.empty())
        return;
    auto column = block.getByName(filter_column).column;
    if (column->isColumnConst())
        column = column->convertToFullColumnIfConst();
    const PaddedPODArray<UInt8> * column_data;
    if (column->isColumnNullable())
    {
        const auto & column_nullable = static_cast<const ColumnNullable &>(*column);
        if (!null_map_holder)
        {
            null_map_holder = column_nullable.getNullMapColumnPtr();
        }
        else
        {
            MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();

            PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
            const PaddedPODArray<UInt8> & other_null_map = column_nullable.getNullMapData();
            for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                mutable_null_map[i] |= other_null_map[i];

            null_map_holder = std::move(mutable_null_map_holder);
        }
        column_data = &static_cast<const ColumnVector<UInt8> *>(column_nullable.getNestedColumnPtr().get())->getData();
    }
    else
    {
        if (!null_map_holder)
        {
            null_map_holder = ColumnVector<UInt8>::create(column->size(), 0);
        }
        column_data = &static_cast<const ColumnVector<UInt8> *>(column.get())->getData();
    }

    MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();
    PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();

    for (size_t i = 0, size = column_data->size(); i < size; ++i)
        mutable_null_map[i] |= !(*column_data)[i];

    null_map_holder = std::move(mutable_null_map_holder);

    null_map = &static_cast<const ColumnUInt8 &>(*null_map_holder).getData();
}

void ProbeProcessInfo::prepareForProbe(const Names & key_names, const String & filter_column, ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness)
{
    if (prepare_for_probe_done)
        return;
    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);
    /// Keys with NULL value in any column won't join to anything.
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter won't join to anything
    recordFilteredRows(block, filter_column, null_map_holder, null_map);
    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (getFullness(kind))
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto & col = block.getByPosition(i).column;

            if (ColumnPtr converted = col->convertToFullColumnIfConst())
                col = converted;

            /// convert left columns (except keys) to Nullable
            if (std::end(key_names) == std::find(key_names.begin(), key_names.end(), block.getByPosition(i).name))
                convertColumnToNullable(block.getByPosition(i));
        }
    }
    if (((kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::RightOuter) && strictness == ASTTableJoin::Strictness::Any)
        || kind == ASTTableJoin::Kind::Anti)
        filter = std::make_unique<IColumn::Filter>(block.rows());
    if (strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(block.rows());
    prepare_for_probe_done = true;
}

namespace
{
UInt64 inline updateHashValue(size_t restore_round, UInt64 x)
{
    static std::vector<UInt64> hash_constants{0xff51afd7ed558ccdULL, 0xc4ceb9fe1a85ec53ULL, 0xde43a68e4d184aa3ULL, 0x86f1fda459fa47c7ULL, 0xd91419add64f471fULL, 0xc18eea9cbe12489eULL, 0x2cb94f36b9fe4c38ULL, 0xef0f50cc5f0c4cbaULL};
    static size_t hash_constants_size = hash_constants.size();
    assert(hash_constants_size > 0 && (hash_constants_size & (hash_constants_size - 1)) == 0);
    assert(restore_round != 0);
    x ^= x >> 33;
    x *= hash_constants[restore_round & (hash_constants_size - 1)];
    x ^= x >> 33;
    x *= hash_constants[(restore_round + 1) & (hash_constants_size - 1)];
    x ^= x >> 33;
    return x;
}
} // namespace
void computeDispatchHash(size_t rows,
                         const ColumnRawPtrs & key_columns,
                         const TiDB::TiDBCollators & collators,
                         std::vector<String> & partition_key_containers,
                         size_t join_restore_round,
                         WeakHash32 & hash)
{
    HashBaseWriterHelper::computeHash(rows, key_columns, collators, partition_key_containers, hash);
    if (join_restore_round != 0)
    {
        auto & data = hash.getData();
        for (size_t i = 0; i < rows; ++i)
            data[i] = updateHashValue(join_restore_round, data[i]);
    }
}

bool mayProbeSideExpandedAfterJoin(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness)
{
    /// null aware semi/left outer semi/anti join never expand the probe side
    if (isNullAwareSemiFamily(kind))
        return false;
    if (isLeftOuterSemiFamily(kind))
        return false;
    if (isAntiJoin(kind))
        return false;
    /// strictness == Any means semi join, it never expand the probe side
    if (strictness == ASTTableJoin::Strictness::Any)
        return false;
    /// for all the other cases, return true by default
    return true;
}
} // namespace DB
