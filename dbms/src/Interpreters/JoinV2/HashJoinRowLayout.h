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

#include <Parsers/ASTTablesInSelectQuery.h>
#include <absl/base/optimization.h>

#include <vector>


namespace DB
{

constexpr size_t ROW_ALIGN = 4;

inline size_t alignRowSize(size_t size)
{
    return (size + ROW_ALIGN - 1) / ROW_ALIGN * ROW_ALIGN;
}

using RowPtr = UInt8 *;
using RowPtrs = PaddedPODArray<RowPtr>;

struct RowContainer
{
    PaddedPODArray<UInt8> data;
    PaddedPODArray<size_t> offsets;

    size_t size() const { return offsets.size(); }

    RowPtr getRowPtr(ssize_t row) { return &data[offsets[row - 1]]; }
};

struct alignas(ABSL_CACHELINE_SIZE) MultipleRowContainer
{
    std::mutex mu;
    std::vector<RowContainer> column_rows;
    size_t all_row_count = 0;

    size_t build_table_index = 0;
    size_t scan_table_index = 0;

    void insert(RowContainer && row_container, size_t count)
    {
        std::unique_lock lock(mu);
        column_rows.push_back(std::move(row_container));
        all_row_count += count;
    }

    RowContainer * getNext()
    {
        std::unique_lock lock(mu);
        if (build_table_index >= column_rows.size())
            return nullptr;
        return &column_rows[build_table_index++];
    }

    RowContainer * getScanNext()
    {
        std::unique_lock lock(mu);
        if (scan_table_index >= column_rows.size())
            return nullptr;
        return &column_rows[scan_table_index++];
    }
};

/// Row Layout
/// 1. No-null join key row: <Hash Value> <Next Pointer> <Raw Join Keys> <Other Join Keys> <Other Columns>
/// 1. Null join key row(For right anti/outer join): <All columns>
struct HashJoinRowLayout
{
    size_t next_pointer_offset;
    size_t join_key_offset;
    std::vector<size_t> key_column_indexes;
    /// The raw join key are the same as the original data.
    /// raw_key_column_index + is_nullable
    std::vector<std::pair<size_t, bool>> raw_key_column_indexes;
    /// other_column_index + is_fixed_size
    std::vector<std::pair<size_t, bool>> other_column_indexes;
    size_t key_column_fixed_size = 0;
    size_t other_column_fixed_size = 0;
    bool join_key_all_raw;

    template <ASTTableJoin::Kind KIND>
    ALWAYS_INLINE inline RowPtr getNextRowPtr(const RowPtr ptr) const
    {
        using enum ASTTableJoin::Kind;
        if constexpr (KIND == RightOuter || KIND == RightSemi || KIND == RightAnti)
        {
            auto * next
                = reinterpret_cast<std::atomic<RowPtr> *>(ptr + next_pointer_offset)->load(std::memory_order_relaxed);
            return reinterpret_cast<RowPtr>(
                reinterpret_cast<uintptr_t>(next) & (~static_cast<uintptr_t>(ROW_ALIGN - 1)));
        }
        return unalignedLoad<RowPtr>(ptr + next_pointer_offset);
    }
};

} // namespace DB
