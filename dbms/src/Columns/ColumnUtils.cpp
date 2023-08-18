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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnUtils.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
bool columnEqual(const ColumnPtr & expected, const ColumnPtr & actual, String & unequal_msg)
{
    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];
        if (expected_field != actual_field)
        {
            unequal_msg
                = fmt::format("Value {} mismatch {} vs {} ", i, expected_field.toString(), actual_field.toString());
            return false;
        }
    }
    return true;
}
void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}
} // namespace DB
