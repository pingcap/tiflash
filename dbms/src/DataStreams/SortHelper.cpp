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

#include <DataStreams/SortHelper.h>

namespace DB::SortHelper
{
void removeConstantsFromBlock(Block & block)
{
    size_t columns = block.columns();
    size_t i = 0;
    while (i < columns)
    {
        if (block.getByPosition(i).column->isColumnConst())
        {
            block.erase(i);
            --columns;
        }
        else
            ++i;
    }
}

void removeConstantsFromSortDescription(const Block & header, SortDescription & description)
{
    description.erase(
        std::remove_if(
            description.begin(),
            description.end(),
            [&](const SortColumnDescription & elem) {
                if (!elem.column_name.empty())
                    return header.getByName(elem.column_name).column->isColumnConst();
                else
                    return header.safeGetByPosition(elem.column_number).column->isColumnConst();
            }),
        description.end());
}

bool isSortByConstants(const Block & header, const SortDescription & description)
{
    return std::all_of(description.begin(), description.end(), [&](const SortColumnDescription & elem) {
        if (!elem.column_name.empty())
            return header.getByName(elem.column_name).column->isColumnConst();
        else
            return header.safeGetByPosition(elem.column_number).column->isColumnConst();
    });
}

void enrichBlockWithConstants(Block & block, const Block & header)
{
    size_t rows = block.rows();
    size_t columns = header.columns();

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col_type_name = header.getByPosition(i);
        if (col_type_name.column->isColumnConst())
            block.insert(i, {col_type_name.column->cloneResized(rows), col_type_name.type, col_type_name.name});
    }
}
} // namespace DB::SortHelper
