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
#include <common/StringRef.h>
#include <common/defines.h>

#include <cstddef>
#include <string_view>


namespace DB
{

template <typename T>
ALWAYS_INLINE inline int signum(T val)
{
    return (0 < val) - (val < 0);
}

// Check equality is much faster than other comparison.
// - check size first
// - return 0 if equal else 1
__attribute__((flatten, always_inline, pure)) inline uint8_t RawStrEqualCompare(const std::string_view & lhs, const std::string_view & rhs)
{
    return StringRef(lhs) == StringRef(rhs) ? 0 : 1;
}

// Compare str view by memcmp
__attribute__((flatten, always_inline, pure)) inline int RawStrCompare(const std::string_view & v1, const std::string_view & v2)
{
    return signum(v1.compare(v2));
}

constexpr char SPACE = ' ';

// Remove tail space
__attribute__((flatten, always_inline, pure)) inline std::string_view RightTrim(const std::string_view & v)
{
    if (likely(v.empty() || v.back() != SPACE))
        return v;
    size_t end = v.find_last_not_of(SPACE);
    return end == std::string_view::npos ? std::string_view{} : std::string_view(v.data(), end + 1);
}

__attribute__((flatten, always_inline, pure)) inline int RtrimStrCompare(const std::string_view & va, const std::string_view & vb)
{
    return RawStrCompare(RightTrim(va), RightTrim(vb));
}

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
template <typename F>
__attribute__((flatten, always_inline)) inline void LoopTwoColumns(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    size_t size,
    F && func)
{
    for (size_t i = 0; i < size; ++i)
    {
        size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
        size_t b_size = StringUtil::sizeAt(b_offsets, i) - 1;
        const auto * a_ptr = reinterpret_cast<const char *>(&a_data[StringUtil::offsetAt(a_offsets, i)]);
        const auto * b_ptr = reinterpret_cast<const char *>(&b_data[StringUtil::offsetAt(b_offsets, i)]);

        func({a_ptr, a_size}, {b_ptr, b_size}, i);
    }
}

// Loop one column and invoke callback for each pair.
template <typename F>
__attribute__((flatten, always_inline)) inline void LoopOneColumn(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    size_t size,
    F && func)
{
    for (size_t i = 0; i < size; ++i)
    {
        size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
        const auto * a_ptr = reinterpret_cast<const char *>(&a_data[StringUtil::offsetAt(a_offsets, i)]);

        func({a_ptr, a_size}, i);
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
