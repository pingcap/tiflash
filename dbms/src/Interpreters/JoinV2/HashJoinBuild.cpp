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

#include <Interpreters/JoinV2/HashJoinBuild.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
} // namespace ErrorCodes

template <typename KeyGetter, bool has_null_map, bool need_record_null_rows>
void NO_INLINE insertBlockToRowContainersTypeImpl(
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    const HashJoinRowLayout & row_layout,
    std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
    JoinBuildWorkerData & wd)
{
    using Type = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    static_assert(sizeof(HashValueType) <= sizeof(decltype(wd.hashes)::value_type));

    Type & key_getter = *static_cast<Type *>(wd.key_getter.get());
    key_getter.reset(key_columns);

    wd.row_sizes.clear();
    wd.row_sizes.resize_fill(rows, row_layout.other_column_fixed_size);
    wd.hashes.resize(rows);
    wd.row_ptrs.clear();
    wd.row_ptrs.resize_fill(rows, nullptr);
    /// The last partition is used to hold rows with null join key.
    constexpr size_t part_count = HJ_BUILD_PARTITION_COUNT + 1;
    wd.partition_row_sizes.clear();
    wd.partition_row_sizes.resize_fill(part_count, 0);
    wd.partition_row_count.clear();
    wd.partition_row_count.resize_fill(part_count, 0);

    for (const auto & [index, is_fixed_size] : row_layout.other_required_column_indexes)
    {
        if (!is_fixed_size)
            block.getByPosition(index).column->countSerializeByteSize(wd.row_sizes);
    }

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
        const auto & key = key_getter.getJoinKeyWithBufferHint(i);

        wd.row_sizes[i] += sizeof(HashValueType) + sizeof(RowPtr) + key_getter.getJoinKeySize(key);
        wd.row_sizes[i] = alignRowSize(wd.row_sizes[i]);
        wd.hashes[i] = static_cast<HashValueType>(Hash()(key));
        size_t part_num = getHJBuildPartitionNum(wd.hashes[i]);
        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        ++wd.partition_row_count[part_num];
    }

    std::vector<RowContainer> partition_column_row(part_count);
    for (size_t i = 0; i < part_count; ++i)
    {
        if (wd.partition_row_count[i] > 0)
        {
            partition_column_row[i].data.resize(wd.partition_row_sizes[i], ROW_ALIGN);
            wd.enable_tagged_pointer &= isRowPtrTagZero(&partition_column_row[i].data.front());
            wd.enable_tagged_pointer &= isRowPtrTagZero(&partition_column_row[i].data.back());

            partition_column_row[i].offsets.reserve(wd.partition_row_count[i]);
            wd.partition_row_sizes[i] = 0;
            wd.row_count += wd.partition_row_count[i];
        }
    }

#ifndef NDEBUG
    RowPtrs check_row_ptrs;
    check_row_ptrs.resize(rows);
#endif

    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if constexpr (need_record_null_rows)
            {
                // FIXME: row size does not correct
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

#ifndef NDEBUG
        check_row_ptrs[i] = wd.row_ptrs[i];
#endif

        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        partition_column_row[part_num].offsets.push_back(wd.partition_row_sizes[part_num]);

        unalignedStore<HashValueType>(wd.row_ptrs[i], static_cast<HashValueType>(wd.hashes[i]));
        wd.row_ptrs[i] += sizeof(HashValueType);
        unalignedStore<RowPtr>(wd.row_ptrs[i], nullptr);
        wd.row_ptrs[i] += sizeof(RowPtr);

        const auto & key = key_getter.getJoinKeyWithBufferHint(i);
        key_getter.serializeJoinKey(key, wd.row_ptrs[i]);
        wd.row_ptrs[i] += key_getter.getJoinKeySize(key);
    }

    if (!row_layout.other_required_column_indexes.empty())
    {
        constexpr size_t step = 64;
        for (size_t i = 0; i < rows; i += step)
        {
            size_t start = i;
            size_t end = i + step > rows ? rows : i + step;
            for (const auto & [index, _] : row_layout.other_required_column_indexes)
            {
                if constexpr (has_null_map && !need_record_null_rows)
                    block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end, true);
                else
                    block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end, false);
            }
        }
    }

#ifndef NDEBUG
    for (size_t i = 0; i < rows; ++i)
    {
        assert(wd.row_ptrs[i] <= check_row_ptrs[i] + wd.row_sizes[i]);
    }
#endif

    for (size_t i = 0; i < part_count; ++i)
    {
        if (wd.partition_row_count[i] > 0)
            multi_row_containers[i]->insert(std::move(partition_column_row[i]), wd.partition_row_count[i]);
    }
}

template <typename KeyGetter>
void insertBlockToRowContainersType(
    bool need_record_null_rows,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    const HashJoinRowLayout & row_layout,
    std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
    JoinBuildWorkerData & worker_data)
{
#define CALL(has_null_map, need_record_null_rows)                                       \
    insertBlockToRowContainersTypeImpl<KeyGetter, has_null_map, need_record_null_rows>( \
        block,                                                                          \
        rows,                                                                           \
        key_columns,                                                                    \
        null_map,                                                                       \
        row_layout,                                                                     \
        multi_row_containers,                                                           \
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

void insertBlockToRowContainers(
    HashJoinKeyMethod method,
    bool need_record_null_rows,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    const HashJoinRowLayout & row_layout,
    std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
    JoinBuildWorkerData & worker_data)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        break;

#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        insertBlockToRowContainersType<KeyGetterType##METHOD>(                             \
            need_record_null_rows,                                                         \
            block,                                                                         \
            rows,                                                                          \
            key_columns,                                                                   \
            null_map,                                                                      \
            row_layout,                                                                    \
            multi_row_containers,                                                          \
            worker_data);                                                                  \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

} // namespace DB
