// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/ColumnString.h>
#include <Core/AccurateComparison.h>
#include <Functions/StringUtil.h>
#include <Storages/Transaction/CollatorUtils.h>
#include <common/StringRef.h>
#include <common/defines.h>

#include <cstddef>
#include <string_view>


namespace DB
{

// If true, only need to check equal or not.
template <typename T>
struct IsEqualRelated
{
    static constexpr const bool value = false;
};

// For `EqualsOp` and `NotEqualsOp`, value is true.
template <typename... A>
struct IsEqualRelated<DB::EqualsOp<A...>>
{
    static constexpr const bool value = true;
};
template <typename... A>
struct IsEqualRelated<DB::NotEqualsOp<A...>>
{
    static constexpr const bool value = true;
};

// Loop columns and invoke callback for each pair.
// Remove last zero byte.
template <typename F>
__attribute__((flatten, always_inline)) inline void LoopTwoColumns(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    size_t size,
    F && func)
{
    ColumnString::Offset a_prev_offset = 0;
    ColumnString::Offset b_prev_offset = 0;

    for (size_t i = 0; i < size; ++i)
    {
        auto a_size = a_offsets[i] - a_prev_offset;
        auto b_size = b_offsets[i] - b_prev_offset;

        // Remove last zero byte.
        func({reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1},
             {reinterpret_cast<const char *>(&b_data[b_prev_offset]), b_size - 1},
             i);

        a_prev_offset = a_offsets[i];
        b_prev_offset = b_offsets[i];
    }
}

// Loop one column and invoke callback for each pair.
// Remove last zero byte.
template <typename F>
__attribute__((flatten, always_inline)) inline void LoopOneColumn(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    size_t size,
    F && func)
{
    ColumnString::Offset a_prev_offset = 0;

    for (size_t i = 0; i < size; ++i)
    {
        auto a_size = a_offsets[i] - a_prev_offset;

        // Remove last zero byte.
        func({reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1}, i);
        a_prev_offset = a_offsets[i];
    }
}

// Handle str-column compare str-column.
// - Optimize UTF8_BIN and UTF8MB4_BIN
//   - Check if columns do NOT contain tail space
//   - If Op is `EqualsOp` or `NotEqualsOp`, optimize comparison by faster way
template <typename Op, typename Result>
ALWAYS_INLINE inline bool StringVectorStringVector(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    bool use_optimized_path = false;

    switch (collator->getCollatorId())
    {
    case TiDB::ITiDBCollator::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::UTF8_BIN:
    {
        size_t size = a_offsets.size();

        LoopTwoColumns(a_data, a_offsets, b_data, b_offsets, size, [&c](const std::string_view & va, const std::string_view & vb, size_t i) {
            if constexpr (IsEqualRelated<Op>::value)
            {
                c[i] = Op::apply(RawStrEqualCompare(RightTrim(va), RightTrim(vb)), 0);
            }
            else
            {
                c[i] = Op::apply(RtrimStrCompare(va, vb), 0);
            }
        });

        use_optimized_path = true;

        break;
    }
    default:
        break;
    }
    return use_optimized_path;
}

// Handle str-column compare const-str.
// - Optimize UTF8_BIN and UTF8MB4_BIN
//   - Right trim const-str first
//   - Check if column does NOT contain tail space
//   - If Op is `EqualsOp` or `NotEqualsOp`, optimize comparison by faster way
template <typename Op, typename Result>
ALWAYS_INLINE inline bool StringVectorConstant(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & b,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    bool use_optimized_path = false;

    switch (collator->getCollatorId())
    {
    case TiDB::ITiDBCollator::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::UTF8_BIN:
    {
        size_t size = a_offsets.size();

        std::string_view tar_str_view = RightTrim(b); // right trim const-str first

        LoopOneColumn(a_data, a_offsets, size, [&c, &tar_str_view](const std::string_view & view, size_t i) {
            if constexpr (IsEqualRelated<Op>::value)
            {
                c[i] = Op::apply(RawStrEqualCompare(RightTrim(view), tar_str_view), 0);
            }
            else
            {
                c[i] = Op::apply(RawStrCompare(RightTrim(view), tar_str_view), 0);
            }
        });

        use_optimized_path = true;
        break;
    }
    default:
        break;
    }
    return use_optimized_path;
}

} // namespace DB
