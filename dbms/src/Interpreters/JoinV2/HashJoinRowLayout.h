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

#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/KVStore/Utils.h>
#include <common/unaligned.h>

#include <atomic>
#include <cstdint>
#include <vector>

namespace DB
{

/// Row Layout
/// 1. if required hash value comparison:
///    <Next Pointer> <Hash Value> <Other Join Keys> <Raw Required Join Keys> <Other Required Columns>
/// 2. if not required hash value comparison:
///    <Next Pointer> <Other Join Keys> <Raw Required Join Keys> <Other Required Columns>
struct HashJoinRowLayout
{
    /// The raw join key column are the same as the original data.
    /// raw_key_column_index in HashJoin::right_sample_block_pruned + is_nullable.
    std::vector<std::pair<size_t, bool>> raw_key_column_indexes;
    /// other_column_index in HashJoin::right_sample_block_pruned + is_fixed_size
    std::vector<std::pair<size_t, bool>> other_column_indexes;
    /// Number of columns at the beginning of `output_other_column_indexes`
    /// that are used for evaluating the join other conditions.
    size_t other_column_count_for_other_condition = 0;

    size_t key_column_fixed_size = 0;
    size_t other_column_fixed_size = 0;
    size_t other_column_for_other_condition_fixed_size = 0;
};

using RowPtr = char *;
using RowPtrs = PaddedPODArray<RowPtr>;

static_assert(alignof(std::atomic<uintptr_t>) == alignof(uintptr_t));
constexpr UInt8 ROW_ALIGN = alignof(uintptr_t);
static_assert((ROW_ALIGN & (ROW_ALIGN - 1)) == 0);
static_assert(ROW_ALIGN >= 4 && ROW_ALIGN < UINT8_MAX);

constexpr size_t ROW_PTR_TAG_BITS = 16;
constexpr size_t ROW_PTR_TAG_MASK = (1 << ROW_PTR_TAG_BITS) - 1;
constexpr size_t ROW_PTR_TAG_SHIFT = 8 * sizeof(RowPtr) - ROW_PTR_TAG_BITS;
static_assert(sizeof(RowPtr) == sizeof(uintptr_t));
static_assert(sizeof(RowPtr) == 8);

template <ASTTableJoin::Kind kind>
inline RowPtr getNextRowPtr(RowPtr ptr)
{
    using enum ASTTableJoin::Kind;
    if constexpr (kind == RightOuter || kind == RightSemi || kind == RightAnti)
    {
        auto next = reinterpret_cast<std::atomic<uintptr_t> *>(ptr)->load(std::memory_order_relaxed);
        return reinterpret_cast<RowPtr>(next & (~static_cast<uintptr_t>(ROW_ALIGN - 1)));
    }
    return *reinterpret_cast<RowPtr *>(ptr);
}

inline UInt8 getRowPtrFlag(RowPtr ptr)
{
    return reinterpret_cast<std::atomic<uintptr_t> *>(ptr)->load(std::memory_order_relaxed)
        & static_cast<uintptr_t>(ROW_ALIGN - 1);
}

inline bool hasRowPtrMatchedFlag(RowPtr ptr)
{
    return getRowPtrFlag(ptr) & 0x01;
}

inline void setRowPtrMatchedFlag(RowPtr ptr)
{
    reinterpret_cast<std::atomic<uintptr_t> *>(ptr)->fetch_or(0x01, std::memory_order_relaxed);
}

inline bool hasRowPtrNullFlag(RowPtr ptr)
{
    return getRowPtrFlag(ptr) & 0x10;
}

inline void setRowPtrNullFlag(RowPtr ptr)
{
    reinterpret_cast<std::atomic<uintptr_t> *>(ptr)->fetch_or(0x10, std::memory_order_relaxed);
}

inline UInt16 getRowPtrTag(RowPtr ptr)
{
    auto address = reinterpret_cast<uintptr_t>(ptr);
    return address >> ROW_PTR_TAG_SHIFT;
}

inline bool isRowPtrTagZero(RowPtr ptr)
{
    return getRowPtrTag(ptr) == 0;
}

inline RowPtr removeRowPtrTag(RowPtr ptr)
{
    auto address = reinterpret_cast<uintptr_t>(ptr);
    address &= (1ULL << ROW_PTR_TAG_SHIFT) - 1;
    return reinterpret_cast<RowPtr>(address);
}

inline bool containOtherTag(RowPtr ptr, UInt16 other_tag)
{
    UInt16 tag = getRowPtrTag(ptr);
    return (tag | other_tag) == tag;
}

struct RowContainer
{
    PaddedPODArray<char> data;
    PaddedPODArray<size_t> offsets;
    PaddedPODArray<UInt64> hashes;
    /// Only used for right semi/anti join
    /// Stores the other columns that are not used for other conditions.
    /// The schema corresponds to the entries in `HashJoinRowLayout::other_column_indexes`
    /// after the first `other_column_count_for_other_condition` elements.
    /// These indexes refer to columns in `HashJoin::right_sample_block_pruned`.
    Block other_column_block;

    size_t size() const { return offsets.size(); }

    RowPtr getRowPtr(ssize_t row) { return &data[offsets[row - 1]]; }
    UInt64 getHash(ssize_t row) { return hashes[row]; }
};

class alignas(CPU_CACHE_LINE_SIZE) MultipleRowContainer
{
public:
    void insert(RowContainer && row_container, size_t count)
    {
        std::unique_lock lock(mu);
        column_rows.push_back(std::move(row_container));
        all_row_count += count;
    }

    RowContainer * getNext()
    {
        if (build_table_done.load(std::memory_order_relaxed))
            return nullptr;
        std::unique_lock lock(mu);
        if (build_table_index >= column_rows.size())
        {
            build_table_done.store(true, std::memory_order_relaxed);
            return nullptr;
        }
        return &column_rows[build_table_index++];
    }

    RowContainer * getScanNext()
    {
        if (scan_table_done.load(std::memory_order_relaxed))
            return nullptr;
        std::unique_lock lock(mu);
        if (scan_table_index >= column_rows.size())
        {
            scan_table_done.store(true, std::memory_order_relaxed);
            return nullptr;
        }
        return &column_rows[scan_table_index++];
    }

private:
    std::mutex mu;
    std::vector<RowContainer> column_rows;
    size_t all_row_count = 0;

    size_t build_table_index = 0;
    size_t scan_table_index = 0;

    std::atomic_bool build_table_done = false;
    std::atomic_bool scan_table_done = false;
};

class NonJoinedBlocks
{
public:
    void insertFullBlock(Block && block)
    {
        std::unique_lock lock(mu);
        full_blocks.push_back(block);
    }

    void insertNonFullBlock(Block && block)
    {
        std::unique_lock lock(mu);
        non_full_blocks.push_back(block);
    }

    Block * getNextFullBlock()
    {
        if (scan_full_blocks_done.load(std::memory_order_relaxed))
            return nullptr;
        std::unique_lock lock(mu);
        if (scan_full_blocks_index >= full_blocks.size())
        {
            scan_full_blocks_done.store(true, std::memory_order_relaxed);
            return nullptr;
        }
        return &full_blocks[scan_full_blocks_index++];
    }

    Block * getNextNonFullBlock()
    {
        if (scan_non_full_blocks_done.load(std::memory_order_relaxed))
            return nullptr;
        std::unique_lock lock(mu);
        if (scan_non_full_blocks_index >= non_full_blocks.size())
        {
            scan_non_full_blocks_done.store(true, std::memory_order_relaxed);
            return nullptr;
        }
        return &non_full_blocks[scan_non_full_blocks_index++];
    }

private:
    std::mutex mu;
    /// Schema: HashJoin::output_block_after_finalize
    /// Each block's size is equal to max_block_size
    std::vector<Block> full_blocks;
    /// Each block's size is less than max_block_size
    std::vector<Block> non_full_blocks;

    size_t scan_full_blocks_index = 0;
    size_t scan_non_full_blocks_index = 0;

    std::atomic_bool scan_full_blocks_done = false;
    std::atomic_bool scan_non_full_blocks_done = false;
};

} // namespace DB
