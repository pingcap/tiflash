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

#include <Functions/CollationStringSearch.h>
#include <Functions/CollationStringSearchOptimized.h>

#include <cstddef>
#include <cstdint>
#include <limits>

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

} // namespace DB