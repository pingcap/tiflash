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

#include <Columns/ColumnsNumber.h>
#include <Core/AccurateComparison.h>
#include <Functions/CollationOperatorOptimized.h>
#include <Functions/CollationStringSearch.h>
#include <Functions/CollationStringSearchOptimized.h>

#include <cstddef>
#include <cstdint>
#include <limits>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

template <bool revert>
bool StringPatternMatch(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & pattern_str,
    uint8_t escape_char,
    const TiDB::TiDBCollatorPtr & collator,
    PaddedPODArray<UInt8> & c)
{
    return StringPatternMatchImpl<revert>(a_data, a_offsets, pattern_str, escape_char, collator, c);
}

template bool StringPatternMatch<true>(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & pattern_str,
    uint8_t escape_char,
    const TiDB::TiDBCollatorPtr & collator,
    PaddedPODArray<UInt8> & c);

template bool StringPatternMatch<false>(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & pattern_str,
    uint8_t escape_char,
    const TiDB::TiDBCollatorPtr & collator,
    PaddedPODArray<UInt8> & c);


#ifdef M
static_assert(false, "`M` is defined");
#endif

template <typename Op, typename Result>
bool CompareStringVectorStringVector(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    return CompareStringVectorStringVectorImpl<Op>(a_data, a_offsets, b_data, b_offsets, collator, c);
}

#define M(OP, Column)                                                                                           \
    template bool CompareStringVectorStringVector<OP<int, int> /*NOLINT*/, PaddedPODArray<Column::value_type>>( \
        const ColumnString::Chars_t & a_data,                                                                   \
        const ColumnString::Offsets & a_offsets,                                                                \
        const ColumnString::Chars_t & b_data,                                                                   \
        const ColumnString::Offsets & b_offsets,                                                                \
        const TiDB::TiDBCollatorPtr & collator,                                                                 \
        PaddedPODArray<Column::value_type> & c)


M(EqualsOp, ColumnUInt8);
M(NotEqualsOp, ColumnUInt8);
M(LessOp, ColumnUInt8);
M(GreaterOp, ColumnUInt8);
M(LessOrEqualsOp, ColumnUInt8);
M(GreaterOrEqualsOp, ColumnUInt8);
M(ReversedCmpOp, ColumnInt8);
M(CmpOp, ColumnInt8);

#undef M

template <typename Op, typename Result>
bool CompareStringVectorConstant(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & _b,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    return CompareStringVectorConstantImpl<Op>(a_data, a_offsets, _b, collator, c);
}

#define M(OP, Column)                                                                                       \
    template bool CompareStringVectorConstant<OP<int, int> /*NOLINT*/, PaddedPODArray<Column::value_type>>( \
        const ColumnString::Chars_t & a_data,                                                               \
        const ColumnString::Offsets & a_offsets,                                                            \
        const std::string_view & _b,                                                                        \
        const TiDB::TiDBCollatorPtr & collator,                                                             \
        PaddedPODArray<Column::value_type> & c)


M(EqualsOp, ColumnUInt8);
M(NotEqualsOp, ColumnUInt8);
M(LessOp, ColumnUInt8);
M(GreaterOp, ColumnUInt8);
M(LessOrEqualsOp, ColumnUInt8);
M(GreaterOrEqualsOp, ColumnUInt8);
M(ReversedCmpOp, ColumnInt8);
M(CmpOp, ColumnInt8);

#undef M


} // namespace DB