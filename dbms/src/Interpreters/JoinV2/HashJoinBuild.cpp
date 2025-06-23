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

#include <Common/typeid_cast.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/HashJoinBuild.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
} // namespace ErrorCodes


template <typename KeyGetter, bool has_null_map, bool need_record_null_rows>
void NO_INLINE JoinBuildHelper::insertBlockToRowContainersImpl(
    HashJoin * join,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    JoinBuildWorkerData & wd,
    bool check_lm_row_size)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    static_assert(sizeof(HashValueType) <= sizeof(decltype(wd.hashes)::value_type));

    const auto kind = join->kind;
    const auto & row_layout = join->row_layout;
    const auto & settings = join->settings;
    auto & multi_row_containers = join->multi_row_containers;
    auto & non_joined_blocks = join->non_joined_blocks;

    auto & key_getter = *static_cast<KeyGetterType *>(wd.key_getter.get());
    key_getter.reset(key_columns, row_layout.raw_key_column_indexes.size());

    RUNTIME_CHECK(multi_row_containers.size() == JOIN_BUILD_PARTITION_COUNT);
    wd.row_sizes.clear();
    bool is_right_semi_family = isRightSemiFamily(kind);
    if (is_right_semi_family)
    {
        wd.row_sizes.resize_fill(rows, row_layout.other_column_for_other_condition_fixed_size);
        wd.right_semi_selector.clear();
        wd.right_semi_selector.reserve(rows);
        if constexpr (has_null_map)
        {
            wd.right_semi_offsets.clear();
            wd.right_semi_offsets.reserve(rows);
        }
    }
    else
    {
        wd.row_sizes.resize_fill(rows, row_layout.other_column_fixed_size);
    }
    if constexpr (has_null_map && need_record_null_rows)
    {
        wd.non_joined_offsets.clear();
        wd.non_joined_offsets.reserve(rows);
    }
    wd.hashes.resize(rows);
    wd.partition_row_sizes.clear();
    wd.partition_row_sizes.resize_fill_zero(JOIN_BUILD_PARTITION_COUNT);
    wd.partition_row_count.clear();
    wd.partition_row_count.resize_fill_zero(JOIN_BUILD_PARTITION_COUNT);
    wd.partition_last_row_index.clear();
    wd.partition_last_row_index.resize_fill(JOIN_BUILD_PARTITION_COUNT, -1);

    if (check_lm_row_size)
    {
        wd.lm_row_count += rows;
        for (size_t i = row_layout.other_column_count_for_other_condition; i < row_layout.other_column_indexes.size();
             ++i)
        {
            size_t index = row_layout.other_column_indexes[i].first;
            const auto & column = block.getByPosition(index).column;
            wd.lm_row_size += column->serializeByteSize();
        }
    }

    size_t other_column_count;
    if (is_right_semi_family)
        other_column_count = row_layout.other_column_count_for_other_condition;
    else
        other_column_count = row_layout.other_column_indexes.size();
    for (size_t i = 0; i < other_column_count; ++i)
    {
        size_t index = row_layout.other_column_indexes[i].first;
        bool is_fixed_size = row_layout.other_column_indexes[i].second;
        if (!is_fixed_size)
            block.getByPosition(index).column->countSerializeByteSize(wd.row_sizes);
    }

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (need_record_null_rows)
                    wd.non_joined_offsets.push_back(i);
                continue;
            }
            if (is_right_semi_family)
                wd.right_semi_offsets.push_back(i);
        }

        const auto & key = key_getter.getJoinKeyWithBuffer(i);
        wd.hashes[i] = static_cast<HashValueType>(Hash()(key));
        size_t part_num = getJoinBuildPartitionNum<HashValueType>(wd.hashes[i]);
        if (is_right_semi_family)
            wd.right_semi_selector.push_back(part_num);

        size_t ptr_and_key_size = sizeof(RowPtr) + key_getter.getJoinKeyByteSize(key);
        if constexpr (KeyGetterType::joinKeyCompareHashFirst())
        {
            ptr_and_key_size += sizeof(HashValueType);
        }
        wd.row_sizes[i] += ptr_and_key_size;

        static_assert(CPU_CACHE_LINE_SIZE % ROW_ALIGN == 0);
        size_t remain_size = CPU_CACHE_LINE_SIZE - wd.partition_row_sizes[part_num] % CPU_CACHE_LINE_SIZE;
        size_t align_remain_size = remain_size / ROW_ALIGN * ROW_ALIGN;
        /// Adding padding to minimize the number of cachelines read during random access to required data in each row.
        if (ptr_and_key_size % CPU_CACHE_LINE_SIZE <= align_remain_size)
        {
            if likely (wd.partition_last_row_index[part_num] != -1)
            {
                size_t diff = remain_size - align_remain_size;
                wd.row_sizes[wd.partition_last_row_index[part_num]] += diff;
                wd.partition_row_sizes[part_num] += diff;
                wd.padding_size += diff;
            }
        }
        else
        {
            if likely (wd.partition_last_row_index[part_num] != -1)
            {
                wd.row_sizes[wd.partition_last_row_index[part_num]] += remain_size;
                wd.partition_row_sizes[part_num] += remain_size;
                wd.padding_size += remain_size;
            }
        }

        wd.partition_last_row_index[part_num] = i;
        wd.partition_row_sizes[part_num] += wd.row_sizes[i];
        ++wd.partition_row_count[part_num];
    }

    std::array<RowContainer, JOIN_BUILD_PARTITION_COUNT> partition_row_container;
    size_t row_count = 0;
    for (size_t i = 0; i < JOIN_BUILD_PARTITION_COUNT; ++i)
    {
        if (wd.partition_row_count[i] > 0)
        {
            auto & container = partition_row_container[i];
            container.data.resize(wd.partition_row_sizes[i], CPU_CACHE_LINE_SIZE);
            wd.enable_tagged_pointer &= isRowPtrTagZero(container.data.data());
            wd.enable_tagged_pointer &= isRowPtrTagZero(container.data.data() + wd.partition_row_sizes[i]);
            RUNTIME_CHECK((reinterpret_cast<uintptr_t>(container.data.data()) & (CPU_CACHE_LINE_SIZE - 1)) == 0);
            wd.all_size += wd.partition_row_sizes[i];

            container.offsets.reserve(wd.partition_row_count[i]);
            if constexpr (!KeyGetterType::joinKeyCompareHashFirst())
                container.hashes.reserve(wd.partition_row_count[i]);

            wd.partition_row_sizes[i] = 0;
            row_count += wd.partition_row_count[i];
        }
    }
    RUNTIME_CHECK(row_count <= rows);
    wd.row_count += row_count;
    wd.non_joined_row_count += rows - row_count;

    constexpr size_t step = 256;
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
                wd.row_ptrs.push_back(nullptr);
                continue;
            }
            size_t part_num = getJoinBuildPartitionNum<HashValueType>(wd.hashes[j]);
            wd.row_ptrs.push_back(partition_row_container[part_num].data.data() + wd.partition_row_sizes[part_num]);
            auto & ptr = wd.row_ptrs.back();
            assert((reinterpret_cast<uintptr_t>(ptr) & (ROW_ALIGN - 1)) == 0);

            wd.partition_row_sizes[part_num] += wd.row_sizes[j];
            partition_row_container[part_num].offsets.push_back(wd.partition_row_sizes[part_num]);

            unalignedStore<RowPtr>(ptr, nullptr);
            ptr += sizeof(RowPtr);

            if constexpr (KeyGetterType::joinKeyCompareHashFirst())
            {
                unalignedStore<HashValueType>(ptr, static_cast<HashValueType>(wd.hashes[j]));
                ptr += sizeof(HashValueType);
            }
            else
            {
                partition_row_container[part_num].hashes.push_back(wd.hashes[j]);
            }
            const auto & key = key_getter.getJoinKeyWithBuffer(j);
            key_getter.serializeJoinKey(key, ptr);
            ptr += key_getter.getJoinKeyByteSize(key);
        }
        for (size_t i = 0; i < other_column_count; ++i)
        {
            size_t index = row_layout.other_column_indexes[i].first;
            if constexpr (has_null_map)
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end - start, true);
            else
                block.getByPosition(index).column->serializeToPos(wd.row_ptrs, start, end - start, false);
        }
    }

    if (is_right_semi_family)
    {
        IColumn::ScatterColumns scatter_columns(JOIN_BUILD_PARTITION_COUNT);
        for (size_t i = row_layout.other_column_count_for_other_condition; i < row_layout.other_column_indexes.size();
             ++i)
        {
            size_t index = row_layout.other_column_indexes[i].first;
            auto & column_data = block.getByPosition(index);
            size_t column_memory = column_data.column->byteSize();
            for (size_t j = 0; j < JOIN_BUILD_PARTITION_COUNT; ++j)
            {
                auto new_column_data = column_data.cloneEmpty();
                if (wd.partition_row_count[j] > 0)
                {
                    size_t memory_hint = 1.2 * column_memory * wd.partition_row_count[j] / rows;
                    new_column_data.column->assumeMutable()->reserveWithTotalMemoryHint(
                        wd.partition_row_count[j],
                        memory_hint);
                }
                scatter_columns[j] = new_column_data.column->assumeMutable();
                partition_row_container[j].other_column_block.insert(std::move(new_column_data));
            }
            if constexpr (has_null_map)
                column_data.column->scatterTo(scatter_columns, wd.right_semi_selector, wd.right_semi_offsets);
            else
                column_data.column->scatterTo(scatter_columns, wd.right_semi_selector);
        }
    }

    if constexpr (has_null_map && need_record_null_rows)
    {
        if (!wd.non_joined_offsets.empty())
        {
            join->initOutputBlock(wd.non_joined_block);
            RUNTIME_CHECK(wd.non_joined_block.rows() < settings.max_block_size);
            size_t columns = wd.non_joined_block.columns();
            auto fill_block = [&](size_t offset_start, size_t length) {
                for (size_t i = 0; i < columns; ++i)
                {
                    auto des_mut_column = wd.non_joined_block.getByPosition(i).column->assumeMutable();
                    const auto & name = wd.non_joined_block.getByPosition(i).name;
                    if (!block.has(name))
                    {
                        // If block does not have this column, this column should be nullable and from the left side
                        RUNTIME_CHECK_MSG(
                            des_mut_column->isColumnNullable(),
                            "Column with name {} is not nullable",
                            name);
                        auto & nullable_column = static_cast<ColumnNullable &>(*des_mut_column);
                        nullable_column.insertManyDefaults(length);
                        continue;
                    }
                    auto & src_column = block.getByName(name).column;
                    des_mut_column->insertSelectiveRangeFrom(*src_column, wd.non_joined_offsets, offset_start, length);
                }
            };
            size_t offset_start = 0;
            size_t offset_size = wd.non_joined_offsets.size();
            while (true)
            {
                size_t remaining_size = settings.max_block_size - wd.non_joined_block.rows();
                if (remaining_size > offset_size - offset_start)
                {
                    fill_block(offset_start, offset_size - offset_start);
                    break;
                }
                fill_block(offset_start, remaining_size);
                offset_start += remaining_size;
                non_joined_blocks.insertFullBlock(std::move(wd.non_joined_block));
                wd.non_joined_block = {};
                if (offset_start >= offset_size)
                    break;
                join->initOutputBlock(wd.non_joined_block);
            }
        }
    }

    for (size_t i = 0; i < JOIN_BUILD_PARTITION_COUNT; ++i)
    {
        if (wd.partition_row_count[i] > 0)
            multi_row_containers[i]->insert(std::move(partition_row_container[i]), wd.partition_row_count[i]);
    }
}

void JoinBuildHelper::insertBlockToRowContainers(
    HashJoin * join,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    JoinBuildWorkerData & wd,
    bool check_lm_row_size)
{
#define CALL2(KeyGetter, has_null_map, need_record_null_rows)                           \
    {                                                                                   \
        insertBlockToRowContainersImpl<KeyGetter, has_null_map, need_record_null_rows>( \
            join,                                                                       \
            block,                                                                      \
            rows,                                                                       \
            key_columns,                                                                \
            null_map,                                                                   \
            wd,                                                                         \
            check_lm_row_size);                                                         \
    }

#define CALL1(KeyGetter)                                                  \
    {                                                                     \
        bool need_record_null_rows = needRecordNotInsertRows(join->kind); \
        if (null_map)                                                     \
        {                                                                 \
            if (need_record_null_rows)                                    \
                CALL2(KeyGetter, true, true)                              \
            else                                                          \
                CALL2(KeyGetter, true, false)                             \
        }                                                                 \
        else                                                              \
            CALL2(KeyGetter, false, false)                                \
    }

    switch (join->method)
    {
#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        CALL1(KeyGetterType##METHOD)                                                       \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception(
            fmt::format("Unknown JOIN keys variant {}.", magic_enum::enum_name(join->method)),
            ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }

#undef CALL1
#undef CALL2
}

} // namespace DB
