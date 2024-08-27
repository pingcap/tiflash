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
#include <Columns/ColumnsCommon.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullableUtils.h>

namespace DB
{
ColumnRawPtrs extractAndMaterializeKeyColumns(
    const Block & block,
    Columns & materialized_columns,
    const Strings & key_columns_names)
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

void recordFilteredRows(
    const Block & block,
    const String & filter_column,
    ColumnPtr & null_map_holder,
    ConstNullMapPtr & null_map)
{
    if (filter_column.empty())
        return;
    auto column = block.getByName(filter_column).column;
    if unlikely (column->onlyNull())
    {
        if (!null_map_holder)
        {
            null_map_holder = ColumnVector<UInt8>::create(column->size(), 1);
        }
        else
        {
            MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();
            PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
            mutable_null_map.resize_fill(column->size(), 1);
            null_map_holder = std::move(mutable_null_map_holder);
        }
        null_map = &static_cast<const ColumnUInt8 &>(*null_map_holder).getData();
        return;
    }

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

namespace
{
UInt64 inline updateHashValue(size_t restore_round, UInt64 x)
{
    static std::vector<UInt64> hash_constants{
        0xff51afd7ed558ccdULL,
        0xc4ceb9fe1a85ec53ULL,
        0xde43a68e4d184aa3ULL,
        0x86f1fda459fa47c7ULL,
        0xd91419add64f471fULL,
        0xc18eea9cbe12489eULL,
        0x2cb94f36b9fe4c38ULL,
        0xef0f50cc5f0c4cbaULL};
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
void computeDispatchHash(
    size_t rows,
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

bool mayProbeSideExpandedAfterJoin(ASTTableJoin::Kind kind)
{
    /// null aware semi/left outer semi/semi/anti/right semi join never expand the probe side
    return !isNullAwareSemiFamily(kind) && !isLeftOuterSemiFamily(kind) && !isSemiFamily(kind)
        && !isRightSemiFamily(kind);
}

std::pair<const PaddedPODArray<UInt8> *, ConstNullMapPtr> getDataAndNullMapVectorFromFilterColumn(
    ColumnPtr & filter_column)
{
    if (filter_column->isColumnConst())
        filter_column = filter_column->convertToFullColumnIfConst();
    if (filter_column->isColumnNullable())
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(filter_column.get());
        const auto & data_column = nullable_column->getNestedColumnPtr();
        return {&checkAndGetColumn<ColumnUInt8>(data_column.get())->getData(), &nullable_column->getNullMapData()};
    }
    else
    {
        return {&checkAndGetColumn<ColumnUInt8>(filter_column.get())->getData(), nullptr};
    }
}

} // namespace DB
