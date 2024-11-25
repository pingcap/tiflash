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
            (*cur_column_file_reader).reset();
            ++cur_column_file_reader;
        }
    }
    return 0;
}

Block ColumnFileSetInputStream::read(FilterPtr & res_filter, bool)
{
    res_filter = nullptr;
    while (cur_column_file_reader != reader.column_file_readers.end())
    {
        if (*cur_column_file_reader == nullptr)
        {
            ++cur_column_file_reader;
            continue;
        }
        auto block = (*cur_column_file_reader)->readNextBlock();
        if (block)
        {
            block.setStartOffset(read_rows);
            read_rows += block.rows();
            return block;
        }
        else
        {
            (*cur_column_file_reader).reset();
            ++cur_column_file_reader;
        }
    }
    return {};
}

Block ColumnFileSetInputStream::readWithFilter(const IColumn::Filter & filter)
{
    while (cur_column_file_reader != reader.column_file_readers.end())
    {
        if (*cur_column_file_reader == nullptr)
        {
            ++cur_column_file_reader;
            continue;
        }
        auto block = (*cur_column_file_reader)->readWithFilter(filter);
        if (block)
        {
            block.setStartOffset(read_rows);
            read_rows += filter.size();
            return block;
        }
        else
        {
            (*cur_column_file_reader).reset();
            ++cur_column_file_reader;
        }
    }
    return {};
}

} // namespace DB::DM
