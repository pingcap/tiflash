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

#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinRowSchema.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <absl/base/optimization.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
} // namespace ErrorCodes


constexpr size_t HJ_BUILD_PARTITION_SHIFT = 4;
constexpr size_t HJ_BUILD_PARTITION_COUNT = 1 << HJ_BUILD_PARTITION_SHIFT;

inline size_t getHJBuildPartitionNum(size_t hash)
{
    constexpr size_t partition_mask = (HJ_BUILD_PARTITION_COUNT - 1) << (32 - HJ_BUILD_PARTITION_SHIFT);
    return (hash & partition_mask) >> (32 - HJ_BUILD_PARTITION_SHIFT);
}

using RowPtr = UInt8 *;
using RowPtrs = PaddedPODArray<RowPtr>;

struct ColumnRow
{
    PaddedPODArray<UInt8> data;
    PaddedPODArray<size_t> offsets;
};

struct alignas(ABSL_CACHELINE_SIZE) ColumnRowsWithLock
{
    std::mutex mu;
    std::vector<ColumnRow> column_rows;
    size_t all_count = 0;
    size_t build_table_index = 0;
    size_t scan_table_index = 0;

    void insert(ColumnRow && row_ptrs, size_t count)
    {
        std::unique_lock lock(mu);
        column_rows.push_back(std::move(row_ptrs));
        all_count += count;
    }

    ColumnRow * getNext()
    {
        std::unique_lock lock(mu);
        if (build_table_index >= column_rows.size())
            return nullptr;
        return &column_rows[build_table_index++];
    }

    ColumnRow * getScanNext()
    {
        std::unique_lock lock(mu);
        if (scan_table_index >= column_rows.size())
            return nullptr;
        return &column_rows[scan_table_index++];
    }
};

struct alignas(ABSL_CACHELINE_SIZE) BuildWorkerData
{
    KeyBuffer key_buffer;
    size_t row_count = 0;

    RowPtr null_rows_list_head = nullptr;

    PaddedPODArray<size_t> row_sizes;
    PaddedPODArray<size_t> hashes;
    RowPtrs row_ptrs;

    PaddedPODArray<size_t> partition_row_sizes;
    PaddedPODArray<size_t> partition_row_count;

    size_t build_time = 0;
    size_t convert_time = 0;

    size_t build_pointer_table_time = 0;
    size_t build_pointer_table_size = 0;

    ssize_t iter_index = -1;
};

template <typename KeyGetter, bool has_null_map, bool need_record_null_rows>
void convertBlockToRowsTypeImpl(
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    ConstNullMapPtr null_map,
    const HashJoinRowSchema & row_schema,
    std::vector<std::unique_ptr<ColumnRowsWithLock>> & partition_column_rows,
    BuildWorkerData & wd)
{
    Stopwatch watch;
    typename KeyGetter::Type key_getter(key_columns, collators, wd.key_buffer);
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    static_assert(sizeof(HashValueType) <= sizeof(decltype(wd.hashes)::value_type));

    wd.row_sizes.clear();
    wd.row_sizes.resize_fill(rows, row_schema.other_column_fixed_size);
    wd.hashes.resize(rows);
    wd.row_ptrs.clear();
    wd.row_ptrs.resize_fill(rows, nullptr);

    for (const auto & [index, is_fixed_size] : row_schema.other_column_indexes)
    {
        if (!is_fixed_size)
            block.getByPosition(index).column->countSerializeByteSize(wd.row_sizes);
    }

    /// The last partition is used to hold rows with null join key.
    constexpr size_t part_count = HJ_BUILD_PARTITION_COUNT + 1;
    wd.partition_row_sizes.resize_fill(part_count, 0);
    wd.partition_row_count.resize_fill(part_count, 0);
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if constexpr (need_record_null_rows)
            {
                wd.row_sizes[i] += sizeof(RowPtr);
                wd.row_sizes[i] = alignRowSize(wd.row_sizes[i]);
                wd.partition_row_sizes[part_count - 1] += wd.row_sizes[i];
                ++wd.partition_row_count[part_count - 1];
            }
            continue;
        }
        const auto & key = key_getter.getJoinKeyForBuild(i);

        wd.row_sizes[i] += sizeof(HashValueType) + sizeof(RowPtr) + key_getter.getJoinKeySize(key);
        wd.row_sizes[i] = alignRowSize(wd.row_sizes[i]);
        wd.hashes[i] = static_cast<HashValueType>(Hash()(key));
        size_t part_num = getHJBuildPartitionNum(wd.hashes[i]);
        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        ++wd.partition_row_count[part_num];
    }

    std::vector<ColumnRow> partition_column_row(part_count);
    for (size_t i = 0; i < part_count; ++i)
    {
        if (wd.partition_row_count[i] > 0)
        {
            partition_column_row[i].data.resize(wd.partition_row_sizes[i], ROW_ALIGN);
            partition_column_row[i].offsets.reserve(wd.partition_row_count[i]);
            wd.partition_row_sizes[i] = 0;
        }
    }

    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if constexpr (need_record_null_rows)
            {
                wd.row_ptrs[i]
                    = partition_column_row[part_count - 1].data.data() + wd.partition_row_sizes[part_count - 1];
                assert((reinterpret_cast<uintptr_t>(wd.row_ptrs[i]) & (ROW_ALIGN - 1)) == 0);

                wd.partition_row_sizes[part_count - 1] += wd.row_sizes[i];
                partition_column_row[part_count - 1].offsets.push_back(wd.partition_row_sizes[part_count - 1]);

                /// Append to null rows list
                unalignedStore<RowPtr>(wd.row_ptrs[i], wd.null_rows_list_head);
                wd.null_rows_list_head = wd.row_ptrs[i];

                wd.row_ptrs[i] += sizeof(RowPtr);
            }
            else
            {
                wd.row_ptrs[i] = nullptr;
            }
            continue;
        }

        size_t part_num = getHJBuildPartitionNum(wd.hashes[i]);
        wd.row_ptrs[i] = partition_column_row[part_num].data.data() + wd.partition_row_sizes[part_num];
        assert((reinterpret_cast<uintptr_t>(wd.row_ptrs[i]) & (ROW_ALIGN - 1)) == 0);

        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        partition_column_row[part_num].offsets.push_back(wd.partition_row_sizes[part_num]);

        unalignedStore<HashValueType>(wd.row_ptrs[i], static_cast<HashValueType>(wd.hashes[i]));
        wd.row_ptrs[i] += sizeof(HashValueType);
        unalignedStore<RowPtr>(wd.row_ptrs[i], nullptr);
        wd.row_ptrs[i] += sizeof(RowPtr);

        const auto & key = key_getter.getJoinKeyForBuild(i);
        key_getter.serializeJoinKey(key, wd.row_ptrs[i]);
        wd.row_ptrs[i] += key_getter.getJoinKeySize(key);
    }

    constexpr size_t step = 128;
    for (size_t i = 0; i < rows; i += step)
    {
        size_t start = i;
        size_t end = i + step > rows ? rows : i + step;
        for (const auto & [index, _] : row_schema.other_column_indexes)
        {
            if constexpr (has_null_map && !need_record_null_rows)
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end, false);
            else
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end, true);
        }
    }

    for (size_t i = 0; i < part_count; ++i)
    {
        if (wd.partition_row_count[i] > 0)
            partition_column_rows[i]->insert(std::move(partition_column_row[i]), wd.partition_row_count[i]);
    }

    wd.convert_time += watch.elapsedMilliseconds();
}

template <typename KeyGetter>
void convertBlockToRowsType(
    bool need_record_null_rows,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    ConstNullMapPtr null_map,
    const HashJoinRowSchema & row_schema,
    std::vector<std::unique_ptr<ColumnRowsWithLock>> & partition_column_rows,
    BuildWorkerData & worker_data)
{
#define CALL(has_null_map, need_record_null_rows)                               \
    convertBlockToRowsTypeImpl<KeyGetter, has_null_map, need_record_null_rows>( \
        block,                                                                  \
        rows,                                                                   \
        key_columns,                                                            \
        collators,                                                              \
        null_map,                                                               \
        row_schema,                                                             \
        partition_column_rows,                                                  \
        worker_data);

    if (null_map)
    {
        if (need_record_null_rows)
        {
            CALL(true, true);
        }
        else
        {
            CALL(true, false);
        }
    }
    else
    {
        CALL(false, false);
    }
#undef CALL
}

void convertBlockToRows(
    HashJoinKeyMethod method,
    bool need_record_null_rows,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    ConstNullMapPtr null_map,
    const HashJoinRowSchema & row_schema,
    std::vector<std::unique_ptr<ColumnRowsWithLock>> & partition_column_rows,
    BuildWorkerData & worker_data)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
        break;
    case HashJoinKeyMethod::Cross:
        break;

#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        convertBlockToRowsType<KeyGetterType##METHOD>(                                     \
            need_record_null_rows,                                                         \
            block,                                                                         \
            rows,                                                                          \
            key_columns,                                                                   \
            collators,                                                                     \
            null_map,                                                                      \
            row_schema,                                                                    \
            partition_column_rows,                                                         \
            worker_data);                                                                  \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}


} // namespace DB