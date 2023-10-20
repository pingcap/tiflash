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

#include <Common/typeid_cast.h>
#include <Core/Types.h>

#include <algorithm>

namespace DB
{
namespace DM
{
struct ColumnAndValue
{
    UInt16 column;
    UInt64 value;

    ColumnAndValue(UInt16 column_, UInt64 value_)
        : column(column_)
        , value(value_)
    {}
};

using ColumnAndValues = std::vector<ColumnAndValue>;

/// A tuple referenced to columns.
struct RefTuple
{
    ColumnAndValues values;

    RefTuple(UInt16 column, UInt64 value)
        : values{ColumnAndValue(column, value)}
    {}

    RefTuple(const ColumnAndValues & values_)
        : values(values_)
    {
        std::sort(values.begin(), values.end(), [](const ColumnAndValue & a, const ColumnAndValue & b) {
            return a.column < b.column;
        });
    }
};

using RefTuples = std::vector<RefTuple>;

} // namespace DM
} // namespace DB
