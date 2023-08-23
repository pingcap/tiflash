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

#include <DataStreams/MockTableScanBlockInputStream.h>

namespace DB
{
MockTableScanBlockInputStream::MockTableScanBlockInputStream(
    ColumnsWithTypeAndName columns,
    size_t max_block_size,
    bool is_infinite_)
    : columns(columns)
    , output_index(0)
    , max_block_size(max_block_size)
    , is_infinite(is_infinite_)
{
    rows = 0;
    for (const auto & elem : columns)
    {
        if (elem.column)
        {
            assert(rows == 0 || rows == elem.column->size());
            rows = elem.column->size();
        }
    }
}

ColumnPtr MockTableScanBlockInputStream::makeColumn(ColumnWithTypeAndName elem) const
{
    auto column = elem.type->createColumn();
    size_t row_count = 0;
    for (size_t i = output_index; (i < rows) & (row_count < max_block_size); ++i)
    {
        column->insert((*elem.column)[i]);
        row_count++;
    }

    return column;
}

Block MockTableScanBlockInputStream::readImpl()
{
    if (output_index >= rows && !is_infinite)
        return {};
    ColumnsWithTypeAndName output_columns;
    for (const auto & elem : columns)
    {
        output_columns.push_back({makeColumn(elem), elem.type, elem.name, elem.column_id});
    }
    output_index += max_block_size;

    return Block(output_columns);
}

} // namespace DB
