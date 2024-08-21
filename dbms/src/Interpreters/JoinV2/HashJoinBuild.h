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

#include <Columns/ColumnNullable.h>
#include <Core/Block.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinRowLayout.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
} // namespace ErrorCodes


constexpr size_t JOIN_BUILD_PARTITION_BITS = 5;
constexpr size_t JOIN_BUILD_PARTITION_COUNT = 1 << JOIN_BUILD_PARTITION_BITS;

template <typename HashValueType>
inline size_t getJoinBuildPartitionNum(HashValueType hash)
{
    constexpr size_t hash_value_bits = sizeof(HashValueType) * 8;
    static_assert(hash_value_bits >= JOIN_BUILD_PARTITION_BITS);
    constexpr size_t partition_mask = (JOIN_BUILD_PARTITION_COUNT - 1) << (hash_value_bits - JOIN_BUILD_PARTITION_BITS);
    return (hash & partition_mask) >> (hash_value_bits - JOIN_BUILD_PARTITION_BITS);
}

struct alignas(CPU_CACHE_LINE_SIZE) JoinBuildWorkerData
{
    std::unique_ptr<void, std::function<void(void *)>> key_getter;
    size_t row_count = 0;

    RowPtr null_rows_list_head = nullptr;

    PaddedPODArray<size_t> row_sizes;
    PaddedPODArray<size_t> hashes;
    RowPtrs row_ptrs;

    PaddedPODArray<size_t> partition_row_sizes;
    PaddedPODArray<size_t> partition_row_count;
    PaddedPODArray<ssize_t> last_partition_index;

    size_t build_time = 0;

    size_t build_pointer_table_time = 0;
    size_t build_pointer_table_size = 0;

    ssize_t build_pointer_table_iter = -1;

    bool enable_tagged_pointer = true;
};

void insertBlockToRowContainers(
    HashJoinKeyMethod method,
    bool need_record_null_rows,
    Block & block,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    const HashJoinRowLayout & row_layout,
    std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
    JoinBuildWorkerData & worker_data);


} // namespace DB
