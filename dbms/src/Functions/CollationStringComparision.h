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

namespace DB
{

template <typename Op, typename Result>
extern bool CompareStringVectorConstant(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & _b,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c);

template <typename Op, typename Result>
extern bool CompareStringVectorStringVector(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const ColumnString::Chars_t & b_data,
    const ColumnString::Offsets & b_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c);

} // namespace DB