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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyReader.h>

namespace DB::DM
{

ColumnPtr ColumnFileTinyReader::getPKColumn()
{
    tiny_file.fillColumns(data_provider, *col_defs, 1, cols_data_cache);
    return cols_data_cache[0];
}

ColumnPtr ColumnFileTinyReader::getVersionColumn()
{
    tiny_file.fillColumns(data_provider, *col_defs, 2, cols_data_cache);
    return cols_data_cache[1];
}

std::pair<size_t, size_t> ColumnFileTinyReader::readRows(
    MutableColumns & output_cols,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    tiny_file.fillColumns(data_provider, *col_defs, output_cols.size(), cols_data_cache);

    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block ColumnFileTinyReader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    tiny_file.fillColumns(data_provider, *col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

size_t ColumnFileTinyReader::skipNextBlock()
{
    if (read_done)
        return 0;

    read_done = true;
    return tiny_file.getRows();
}

ColumnFileReaderPtr ColumnFileTinyReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Reuse the cache data.
    return std::make_shared<ColumnFileTinyReader>(tiny_file, data_provider, new_col_defs, cols_data_cache);
}

} // namespace DB::DM
