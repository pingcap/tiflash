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

#include <Storages/KVStore/Utils.h>

#include <vector>

namespace DB
{

constexpr size_t ROW_ALIGN = 8;

using RowPtr = char *;
using RowPtrs = PaddedPODArray<RowPtr>;

struct RowContainer
{
    PaddedPODArray<char> data;
    PaddedPODArray<size_t> offsets;
    PaddedPODArray<size_t> hashes;

    size_t size() const { return offsets.size(); }

    RowPtr getRowPtr(ssize_t row) { return &data[offsets[row - 1]]; }
    size_t getHash(ssize_t row) { return hashes[row]; }
};

struct alignas(CPU_CACHE_LINE_SIZE) MultipleRowContainer
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
/// 1. No-null join key row: <Hash Value> <Next Pointer> <Other Join Keys> <Raw Required Join Keys> <Other Required Columns>
/// 1. Null join key row(For right anti/outer join): <All Required Columns>
struct HashJoinRowLayout
{
    /// The raw join key are the same as the original data.
    /// raw_required_key_column_index + is_nullable
    std::vector<std::pair<size_t, bool>> raw_required_key_column_indexes;
    /// other_required_column_index + is_fixed_size
    std::vector<std::pair<size_t, bool>> other_required_column_indexes;
    size_t key_column_fixed_size = 0;
    size_t other_column_fixed_size = 0;

    static RowPtr getNextRowPtr(const RowPtr ptr) { return unalignedLoad<RowPtr>(ptr); }
};

constexpr size_t ROW_PTR_TAG_BITS = 16;
constexpr size_t ROW_PTR_TAG_MASK = (1 << ROW_PTR_TAG_BITS) - 1;

inline UInt16 getRowPtrTag(RowPtr ptr)
{
    static_assert(sizeof(RowPtr) == 8);
    auto address = reinterpret_cast<uintptr_t>(ptr);
    return address >> (64 - ROW_PTR_TAG_BITS);
}

inline bool isRowPtrTagZero(RowPtr ptr)
{
    return getRowPtrTag(ptr) == 0;
}

inline RowPtr removeRowPtrTag(RowPtr ptr)
{
    auto address = reinterpret_cast<uintptr_t>(ptr);
    address &= (1ULL << (64 - ROW_PTR_TAG_BITS)) - 1;
    return reinterpret_cast<RowPtr>(address);
}

inline RowPtr addRowPtrTag(RowPtr ptr, UInt16 tag)
{
    static_assert(sizeof(uintptr_t) == 8);
    auto address = reinterpret_cast<uintptr_t>(ptr);
    address |= static_cast<uintptr_t>(tag) << (64 - ROW_PTR_TAG_BITS);
    return reinterpret_cast<RowPtr>(address);
}

inline bool containOtherTag(RowPtr ptr, UInt16 other_tag)
{
    UInt16 tag = getRowPtrTag(ptr);
    return (tag | other_tag) == tag;
}

} // namespace DB
