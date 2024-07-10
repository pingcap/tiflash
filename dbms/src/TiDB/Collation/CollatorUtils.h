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

#pragma once

#include <Common/Exception.h>
#include <Common/WeakHash.h>
#include <Core/BlockInfo.h>
#include <TiDB/Collation/Collator.h>
#include <TiDB/Collation/CollatorCompare.h>

namespace DB
{

// Loop columns and invoke callback for each pair.
// Remove last zero byte.
template <typename Chars, typename Offsets, typename F>
FLATTEN_INLINE static inline void LoopTwoColumns(
    const Chars & a_data,
    const Offsets & a_offsets,
    const Chars & b_data,
    const Offsets & b_offsets,
    size_t size,
    F && func)
{
    uint64_t a_prev_offset = 0;
    uint64_t b_prev_offset = 0;

    for (size_t i = 0; i < size; ++i)
    {
        auto a_size = a_offsets[i] - a_prev_offset;
        auto b_size = b_offsets[i] - b_prev_offset;

        // Remove last zero byte.
        func(
            {reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1},
            {reinterpret_cast<const char *>(&b_data[b_prev_offset]), b_size - 1},
            i);

        a_prev_offset = a_offsets[i];
        b_prev_offset = b_offsets[i];
    }
}

// Loop one column and invoke callback for each pair.
// Remove last zero byte.
template <typename Chars, typename Offsets, typename F>
FLATTEN_INLINE static inline void LoopOneColumn(const Chars & a_data, const Offsets & a_offsets, size_t size, F && func)
{
    uint64_t a_prev_offset = 0;

    for (size_t i = 0; i < size; ++i)
    {
        auto a_size = a_offsets[i] - a_prev_offset;

        // Remove last zero byte.
        func({reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1}, i);
        a_prev_offset = a_offsets[i];
    }
}


// Used when updating hash for column.
struct WeakHash32Info
{
    WeakHash32::Container * hash_data;
    String sort_key_container;
    TiDB::TiDBCollatorPtr collator;
    BlockSelectivePtr selective_ptr;
};

// Loop one column and invoke callback for each pair.
// Remove last zero byte.
template <typename Chars, typename Offsets, typename Func, bool selective>
FLATTEN_INLINE static inline void LoopOneColumnWithHashInfo(
    const Chars & a_data,
    const Offsets & a_offsets,
    WeakHash32Info & info,
    const Func & func)
{
    size_t rows = a_offsets.size();
    if constexpr (selective)
    {
        RUNTIME_CHECK(info.selective_ptr);
        rows = info.selective_ptr->size();
    }

    RUNTIME_CHECK_MSG(
        info.hash_data->size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        info.hash_data->size(),
        rows);

    for (size_t i = 0; i < rows; ++i)
    {
        size_t row = i;
        if constexpr (selective)
            row = (*info.selective_ptr)[i];

        size_t a_prev_offset = 0;
        if likely (row > 0)
            a_prev_offset = a_offsets[row - 1];

        auto a_size = a_offsets[row] - a_prev_offset;

        func({reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1}, i, info);
    }
}
} // namespace DB
