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

#include <Core/BlockUtils.h>

namespace DB
{
bool blockEqual(const Block & expected, const Block & actual, String & unequal_msg)
{
    if (size_t rows_a = expected.rows(), rows_b = actual.rows(); rows_a != rows_b)
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

    for (size_t i = 0; i < size_a; ++i)
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
    FmtBuffer buf;
    for (size_t i = 0; i < rows; ++i)
    {
        buf.joinStr(
            block.begin(),
            block.end(),
            [i](const auto & block, FmtBuffer & fb) {
                auto column = block.column;
                auto field = (*column)[i];
                fb.append(field.toString());
            },
            ", ");
    }
    return buf.toString();
}
} // namespace DB
