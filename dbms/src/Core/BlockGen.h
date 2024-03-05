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

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/Buffer/ReadBufferFromString.h>

namespace DB
{
// Convenient methods for generating a block. Normally used by test cases.

using CSVTuple = std::vector<String>;
using CSVTuples = std::vector<CSVTuple>;

Block genBlock(const Block & header, const CSVTuples & tuples)
{
    Block block;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & cd = header.getByPosition(i);
        ColumnWithTypeAndName col{{}, cd.type, cd.name, cd.column_id};
        auto col_data = cd.type->createColumn();
        for (const auto & tuple : tuples)
        {
            ReadBufferFromString buf(tuple.at(i));
            cd.type->deserializeTextCSV(*col_data, buf, '|');
        }
        col.column = std::move(col_data);
        block.insert(std::move(col));
    }
    return block;
}

} // namespace DB
