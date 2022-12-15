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

#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny2.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB
{
namespace DM
{

Columns ColumnFileTiny2::readFromDisk(
    const IColumnFileSetStorageReaderPtr & storage_reader,
    const ColumnDefines & column_defines,
    size_t col_start,
    size_t col_end) const
{
    const size_t num_columns_read = col_end - col_start;

    Columns columns(num_columns_read); // allocate empty columns

    UniversalPageStorage::PageReadFields fields;
    fields.first = data_page_id;
    for (size_t index = col_start; index < col_end; ++index)
    {
        const auto & cd = column_defines[index];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_index = it->second;
            fields.second.push_back(col_index);
        }
        else
        {
            // New column after ddl is not exist in this pack, fill with default value
            columns[index - col_start] = createColumnWithDefaultValue(cd, rows);
        }
    }

    auto page_map = storage_reader->readForColumnFileTiny2({fields});
    UniversalPage page = page_map[data_page_id];
    for (size_t index = col_start; index < col_end; ++index)
    {
        const size_t index_in_read_columns = index - col_start;
        if (columns[index_in_read_columns] != nullptr)
        {
            // the column is fill with default values.
            continue;
        }
        auto col_id = column_defines[index].id;
        auto col_index = colid_to_offset.at(col_id);
        auto data_buf = page.getFieldData(col_index);

        const auto & cd = column_defines[index];
        // Deserialize column by pack's schema
        const auto & type = getDataType(cd.id);
        auto col_data = type->createColumn();
        deserializeColumn(*col_data, type, data_buf, rows);

        columns[index_in_read_columns] = convertColumnByColumnDefineIfNeed(type, std::move(col_data), cd);
    }

    return columns;
}

void ColumnFileTiny2::fillColumns(
    const IColumnFileSetStorageReaderPtr & storage_reader,
    const ColumnDefines & col_defs,
    size_t col_count,
    Columns & result) const
{
    if (result.size() >= col_count)
        return;

    size_t col_start = result.size();
    size_t col_end = col_count;

    Columns read_cols = readFromDisk(storage_reader, col_defs, col_start, col_end);
    result.insert(result.end(), read_cols.begin(), read_cols.end());
}

ColumnFileReaderPtr ColumnFileTiny2::getReader(
    const DMContext &,
    const IColumnFileSetStorageReaderPtr & reader,
    const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<ColumnFileTiny2Reader>(*this, reader, col_defs);
}

ColumnPtr ColumnFileTiny2Reader::getPKColumn()
{
    tiny_file.fillColumns(storage_reader, *col_defs, 1, cols_data_cache);
    return cols_data_cache[0];
}

ColumnPtr ColumnFileTiny2Reader::getVersionColumn()
{
    tiny_file.fillColumns(storage_reader, *col_defs, 2, cols_data_cache);
    return cols_data_cache[1];
}

size_t ColumnFileTiny2Reader::readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    tiny_file.fillColumns(storage_reader, *col_defs, output_cols.size(), cols_data_cache);
    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block ColumnFileTiny2Reader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    tiny_file.fillColumns(storage_reader, *col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

ColumnFileReaderPtr ColumnFileTiny2Reader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Reuse the cache data.
    return std::make_shared<ColumnFileTiny2Reader>(tiny_file, storage_reader, new_col_defs, cols_data_cache);
}

} // namespace DM
} // namespace DB