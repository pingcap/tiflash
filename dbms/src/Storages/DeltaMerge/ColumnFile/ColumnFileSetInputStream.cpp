// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetInputStream.h>

namespace DB::DM
{

size_t ColumnFileSetInputStream::skipNextBlock()
{
    while (cur_column_file_reader != reader.column_file_readers.end())
    {
        if (*cur_column_file_reader == nullptr)
        {
            ++cur_column_file_reader;
            continue;
        }
        auto skipped_rows = (*cur_column_file_reader)->skipNextBlock();
        read_rows += skipped_rows;
        if (skipped_rows)
            return skipped_rows;
        else
        {
            auto prev = *cur_column_file_reader;
            ++cur_column_file_reader;
            prev.reset();
        }
    }
    return 0;
}

Block ColumnFileSetInputStream::read()
{
    while (cur_column_file_reader != reader.column_file_readers.end())
    {
        if (*cur_column_file_reader == nullptr)
        {
            ++cur_column_file_reader;
            continue;
        }
        auto block = (*cur_column_file_reader)->readNextBlock();
        read_rows += block.rows();
        if (block.rows())
            return block;
        else
        {
            auto prev = *cur_column_file_reader;
            ++cur_column_file_reader;
            prev.reset();
        }
    }
    return {};
}

Block ColumnFileSetInputStream::readWithFilter(const IColumn::Filter & filter)
{
    auto block = read();
    if (size_t passed_count = countBytesInFilter(filter); passed_count != block.rows())
    {
        for (auto & col : block)
        {
            col.column = col.column->filter(filter, passed_count);
        }
    }
    return block;
}

} // namespace DB::DM
