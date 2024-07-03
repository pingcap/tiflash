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


constexpr size_t HJ_BUILD_PARTITION_SHIFT = 4;
constexpr size_t HJ_BUILD_PARTITION_COUNT = 1 << HJ_BUILD_PARTITION_SHIFT;

inline size_t getHJBuildPartitionNum(size_t hash)
{
    constexpr size_t partition_mask = (HJ_BUILD_PARTITION_COUNT - 1) << (32 - HJ_BUILD_PARTITION_SHIFT);
    return (hash & partition_mask) >> (32 - HJ_BUILD_PARTITION_SHIFT);
}

struct alignas(ABSL_CACHELINE_SIZE) JoinBuildWorkerData
{
    std::unique_ptr<void, std::function<void(void *)>> key_getter;
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

    ssize_t build_pointer_table_iter = -1;
};

void insertBlockToRowContainers(
    HashJoinKeyMethod method,
    bool need_record_null_rows,
    Block & block,
    const ColumnRawPtrs & key_columns,
    ConstNullMapPtr null_map,
    const HashJoinRowLayout & row_layout,
    std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
    JoinBuildWorkerData & worker_data);


} // namespace DB