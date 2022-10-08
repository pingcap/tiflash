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

#include <Core/BlockUtils.h>

namespace DB
{
bool blockEqual(const Block & expected, const Block & actual, String & unequal_msg)
{
    size_t rows_a = expected.rows();
    size_t rows_b = actual.rows();
    if (rows_a != rows_b)
    {
        unequal_msg = fmt::format("Row counter are not equal: {} vs {} ", rows_a, rows_b);
        return false;
    }

    size_t size_a = expected.columns();
    size_t size_b = actual.columns();
    if (size_a != size_b)
    {
        unequal_msg = fmt::format("Columns size are not equal: {} vs {} ", size_a, size_b);
        return false;
    }

    for (size_t i = 0; i < size_a; i++)
    {
        bool equal = columnEqual(expected.getByPosition(i).column, actual.getByPosition(i).column, unequal_msg);
        if (!equal)
        {
            unequal_msg = fmt::format("{}th columns are not equal, details: {}", i, unequal_msg);
            return false;
        }
    }
    return true;
}

String formatBlockData(const Block & block)
{
    size_t rows = block.rows();
    size_t columns = block.columns();
    String result;
    for (size_t i = 0; i < rows; i++)
    {
        for (size_t j = 0; j < columns; j++)
        {
            auto column = block.getByPosition(j).column;
            auto field = (*column)[i];
            if (j + 1 < columns)
            {
                // Just use "," as separator now, maybe confusing when string column contains ","
                result += fmt::format("{},", field.toString());
            }
            else
            {
                result += fmt::format("{}\n", field.toString());
            }
        }
    }
    return result;
}
} // namespace DB
