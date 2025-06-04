// Copyright 2025 PingCAP, Inc.
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

#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB::DM
{

size_t ColumnFileInputStream::skipNextBlock()
{
    if (!reader)
        return 0;
    return reader->skipNextBlock();
}

Block ColumnFileInputStream::readWithFilter(const IColumn::Filter & filter)
{
    if (!reader)
        return {};
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

Block ColumnFileInputStream::getHeader() const
{
    return toEmptyBlock(*col_defs);
}

Block ColumnFileInputStream::read()
{
    if (!reader)
        return {};
    return reader->readNextBlock();
}

ColumnFileInputStream::ColumnFileInputStream(
    const DMContext & context_,
    const ColumnFilePtr & column_file,
    const IColumnFileDataProviderPtr & data_provider_,
    const ColumnDefinesPtr & col_defs_,
    ReadTag read_tag_)
    : col_defs(col_defs_)
    // Note that ColumnFileDelete does not have a reader, so that the reader will be nullptr.
    , reader(column_file->getReader(context_, data_provider_, col_defs_, read_tag_))
{
    RUNTIME_CHECK(col_defs != nullptr);
}

} // namespace DB::DM
