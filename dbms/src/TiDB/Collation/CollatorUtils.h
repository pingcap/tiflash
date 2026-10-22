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

#include <TiDB/Collation/CollatorCompare.h>

#include <algorithm>

namespace DB
{

/// Upper bound for the weights emitted by GeneralCI and UnicodeCI.
inline size_t estimateSortKeyByteSize(const char * data, size_t size, size_t max_bytes_one_char)
{
    size_t num_char = 0;
    size_t offset = 0;
    // Skip ASCII blocks before walking multibyte sequences.
    while (size - offset >= sizeof(uint64_t))
    {
        uint64_t word;
        std::memcpy(&word, data + offset, sizeof(word));
        if (word & 0x8080808080808080ULL)
            break;
        offset += sizeof(word);
        num_char += sizeof(word);
    }
    while (offset < size)
    {
        // Keep byte advances aligned with decodeUtf8Char without decoding collation weights.
        const auto first = static_cast<unsigned char>(data[offset]);
        const size_t step = first < 0x80 ? 1 : first < 0xE0 ? 2 : first < 0xF0 ? 3 : 4;
        offset += std::min(step, size - offset);
        ++num_char;
    }
    return num_char * max_bytes_one_char;
}

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

} // namespace DB
