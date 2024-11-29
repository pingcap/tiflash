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
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    static_assert(sizeof(HashValueType) <= sizeof(decltype(wd.hashes)::value_type));

    KeyGetterType & key_getter = *static_cast<KeyGetterType *>(wd.key_getter.get());
    key_getter.reset(key_columns, row_layout.raw_required_key_column_indexes.size());

    wd.row_sizes.clear();
    wd.row_sizes.resize_fill(rows, row_layout.other_column_fixed_size);
    wd.hashes.resize(rows);
    /// The last partition is used to hold rows with null join key.
    constexpr size_t part_count = JOIN_BUILD_PARTITION_COUNT + 1;
    wd.partition_row_sizes.clear();
    wd.partition_row_sizes.resize_fill(part_count, 0);
    wd.partition_row_count.clear();
    wd.partition_row_count.resize_fill(part_count, 0);
    wd.last_partition_index.clear();
    wd.last_partition_index.resize_fill(part_count, -1);

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
                //TODO
                //wd.row_sizes[i] += sizeof(RowPtr);
                //wd.row_sizes[i] = alignRowSize(wd.row_sizes[i]);
                //wd.partition_row_sizes[part_count - 1] += wd.row_sizes[i];
                //++wd.partition_row_count[part_count - 1];
            }
            continue;
        }
        const auto & key = key_getter.getJoinKeyWithBufferHint(i);
        wd.hashes[i] = static_cast<HashValueType>(Hash()(key));
        size_t part_num = getJoinBuildPartitionNum<HashValueType>(wd.hashes[i]);

        size_t ptr_and_key_size = sizeof(RowPtr) + key_getter.getJoinKeySize(key);
        if constexpr (KeyGetterType::joinKeyCompareHashFirst())
        {
            ptr_and_key_size += sizeof(HashValueType);
        }
        wd.row_sizes[i] += ptr_and_key_size;

        static_assert(CPU_CACHE_LINE_SIZE % ROW_ALIGN == 0);
        size_t remain_size = CPU_CACHE_LINE_SIZE - wd.partition_row_sizes[part_num] % CPU_CACHE_LINE_SIZE;
        size_t align_remain_size = remain_size / ROW_ALIGN * ROW_ALIGN;
        if (ptr_and_key_size % CPU_CACHE_LINE_SIZE <= align_remain_size)
        {
            if likely (wd.last_partition_index[part_num] != -1)
            {
                size_t diff = remain_size - align_remain_size;
                wd.row_sizes[wd.last_partition_index[part_num]] += diff;
                wd.partition_row_sizes[part_num] += diff;
                wd.padding_size += diff;
            }
        }
        else
        {
            if likely (wd.last_partition_index[part_num] != -1)
            {
                wd.row_sizes[wd.last_partition_index[part_num]] += remain_size;
                wd.partition_row_sizes[part_num] += remain_size;
                wd.padding_size += remain_size;
            }
        }

        wd.last_partition_index[part_num] = i;
        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        ++wd.partition_row_count[part_num];
    }

    std::vector<RowContainer> partition_column_row(part_count);
    for (size_t i = 0; i < part_count; ++i)
    {
        if (wd.partition_row_count[i] > 0)
        {
            auto & container = partition_column_row[i];
            container.data.resize(wd.partition_row_sizes[i], CPU_CACHE_LINE_SIZE);
            wd.enable_tagged_pointer &= isRowPtrTagZero(container.data.data());
            wd.enable_tagged_pointer &= isRowPtrTagZero(container.data.data() + wd.partition_row_sizes[i]);
            assert((reinterpret_cast<uintptr_t>(container.data.data()) & (CPU_CACHE_LINE_SIZE - 1)) == 0);
            wd.all_size += wd.partition_row_sizes[i];

            container.offsets.reserve(wd.partition_row_count[i]);
            if constexpr (!KeyGetterType::joinKeyCompareHashFirst())
                container.hashes.reserve(wd.partition_row_count[i]);

            wd.partition_row_sizes[i] = 0;
            wd.row_count += wd.partition_row_count[i];
        }
    }

    constexpr size_t step = 64;
    wd.row_ptrs.reserve(step);
    for (size_t i = 0; i < rows; i += step)
    {
        wd.row_ptrs.clear();

        size_t start = i;
        size_t end = i + step > rows ? rows : i + step;
        for (size_t j = start; j < end; ++j)
        {
            if (has_null_map && (*null_map)[j])
            {
                if constexpr (need_record_null_rows)
                {
                    //TODO
                }
                else
                {
                    wd.row_ptrs.push_back(nullptr);
                }
                continue;
            }
            size_t part_num = getJoinBuildPartitionNum<HashValueType>(wd.hashes[j]);
            wd.row_ptrs.push_back(partition_column_row[part_num].data.data() + wd.partition_row_sizes[part_num]);
            auto & ptr = wd.row_ptrs.back();
            assert((reinterpret_cast<uintptr_t>(ptr) & (ROW_ALIGN - 1)) == 0);

            wd.partition_row_sizes[part_num] += wd.row_sizes[j];
            partition_column_row[part_num].offsets.push_back(wd.partition_row_sizes[part_num]);

            unalignedStore<RowPtr>(ptr, nullptr);
            ptr += sizeof(RowPtr);

            if constexpr (KeyGetterType::joinKeyCompareHashFirst())
            {
                unalignedStore<HashValueType>(ptr, static_cast<HashValueType>(wd.hashes[j]));
                ptr += sizeof(HashValueType);
            }
            else
            {
                partition_column_row[part_num].hashes.push_back(wd.hashes[j]);
            }
            const auto & key = key_getter.getJoinKeyWithBufferHint(j);
            key_getter.serializeJoinKey(key, ptr);
            ptr += key_getter.getJoinKeySize(key);
        }
        for (const auto & [index, _] : row_layout.other_required_column_indexes)
        {
            if constexpr (has_null_map && !need_record_null_rows)
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end - start, true);
            else
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end - start, false);
        }
    }

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
