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

#include <Columns/ColumnString.h>
#include <Core/AccurateComparison.h>
#include <Functions/StringUtil.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/fixed_mem_eq.h>

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

template <typename Op, bool trim, typename Result, typename F>
FLATTEN_INLINE static inline void LoopOneColumnCmpEqStr(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    Result & c,
    F && fn_is_eq)
{
    LoopOneColumn(a_data, a_offsets, a_offsets.size(), [&](std::string_view view, size_t i) {
        if constexpr (trim)
            view = RightTrim(view);
        auto res = fn_is_eq(view) ? 0 : 1;
        c[i] = Op::apply(res, 0);
    });
}

// Handle str-column compare str-column.
// - Optimize bin collator
//   - Check if columns do NOT contain tail space
//   - If Op is `EqualsOp` or `NotEqualsOp`, optimize comparison by faster way
template <typename Op, typename Result>
ALWAYS_INLINE inline bool CompareStringVectorStringVectorImpl(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    bool use_optimized_path = false;

    switch (collator->getCollatorType())
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    {
        size_t size = a_offsets.size();

        LoopTwoColumns(
            a_data,
            a_offsets,
            b_data,
            b_offsets,
            size,
            [&c](const std::string_view & va, const std::string_view & vb, size_t i) {
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
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    {
        size_t size = a_offsets.size();

        LoopTwoColumns(
            a_data,
            a_offsets,
            b_data,
            b_offsets,
            size,
            [&c](const std::string_view & va, const std::string_view & vb, size_t i) {
                if constexpr (IsEqualRelated<Op>::value)
                {
                    c[i] = Op::apply(RawStrEqualCompare((va), (vb)), 0);
                }
                else
                {
                    c[i] = Op::apply(RawStrCompare(va, vb), 0);
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

template <bool need_trim, typename Op, typename Result>
ALWAYS_INLINE static inline bool BinCollatorCompareStringVectorConstant(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & tar_str_view,
    Result & c)
{
    if constexpr (!IsEqualRelated<Op>::value)
        return false;

#ifdef M
    static_assert(false, "`M` is defined");
#endif
#define M(k)                                                                                                     \
    case k:                                                                                                      \
    {                                                                                                            \
        LoopOneColumnCmpEqStr<Op, need_trim>(a_data, a_offsets, c, [&](const std::string_view & src) -> bool {   \
            return (src.size() == (k)) && mem_utils::memcmp_eq_fixed_size<(k)>(src.data(), tar_str_view.data()); \
        });                                                                                                      \
        return true;                                                                                             \
    }
    if (likely(tar_str_view.size() <= 32))
    {
        switch (tar_str_view.size())
        {
            M(0);
            M(1);
            M(2);
            M(3);
            M(4);
            M(5);
            M(6);
            M(7);
            M(8);
            M(9);
            M(10);
            M(11);
            M(12);
            M(13);
            M(14);
            M(15);
            M(16);
        default:
        {
            LoopOneColumnCmpEqStr<Op, need_trim>(a_data, a_offsets, c, [&](const std::string_view & src) -> bool {
                const size_t n = tar_str_view.size();
                if (src.size() != n)
                    return false;
                const auto * p1 = src.data();
                const auto * p2 = tar_str_view.data();
                return mem_utils::memcmp_eq_fixed_size<16>(p1, p2)
                    && mem_utils::memcmp_eq_fixed_size<16>(p1 + n - 16, p2 + n - 16);
            });
            return true;
        }
        }
    }
#undef M
    return false;
}

// Handle str-column compare const-str.
// - Optimize bin collator
//   - Right trim const-str first
//   - Check if column does NOT contain tail space
//   - If Op is `EqualsOp` or `NotEqualsOp`, optimize comparison by faster way
template <typename Op, typename Result>
ALWAYS_INLINE static inline bool CompareStringVectorConstantImpl(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & _b,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    switch (collator->getCollatorType())
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    {
        std::string_view tar_str_view = RightTrim(_b); // right trim const-str first

        if (BinCollatorCompareStringVectorConstant<true, Op>(a_data, a_offsets, tar_str_view, c))
            return true;

        LoopOneColumn(
            a_data,
            a_offsets,
            a_offsets.size(),
            [&c, &tar_str_view](const std::string_view & view, size_t i) {
                if constexpr (IsEqualRelated<Op>::value)
                {
                    c[i] = Op::apply(RawStrEqualCompare(RightTrim(view), tar_str_view), 0);
                }
                else
                {
                    c[i] = Op::apply(RawStrCompare(RightTrim(view), tar_str_view), 0);
                }
            });

        return true;
    }
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    {
        const std::string_view & tar_str_view = _b; // use original const-str

        if (BinCollatorCompareStringVectorConstant<false, Op>(a_data, a_offsets, tar_str_view, c))
            return true;

        LoopOneColumn(
            a_data,
            a_offsets,
            a_offsets.size(),
            [&c, &tar_str_view](const std::string_view & view, size_t i) {
                if constexpr (IsEqualRelated<Op>::value)
                {
                    c[i] = Op::apply(RawStrEqualCompare(view, tar_str_view), 0);
                }
                else
                {
                    c[i] = Op::apply(RawStrCompare(view, tar_str_view), 0);
                }
            });

        return true;
    }
    default:
        break;
    }
    return false;
}

} // namespace DB
