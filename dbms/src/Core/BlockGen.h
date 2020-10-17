#pragma once

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>

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
        auto & cd = header.getByPosition(i);
        ColumnWithTypeAndName col{{}, cd.type, cd.name, cd.column_id};
        auto col_data = cd.type->createColumn();
        for (auto & tuple : tuples)
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