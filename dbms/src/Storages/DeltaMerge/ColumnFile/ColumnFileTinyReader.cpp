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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Columns/ColumnsCommon.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyReader.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>


namespace DB::DM
{

std::pair<ColumnPtr, ColumnPtr> ColumnFileTinyReader::getPKAndVersionColumns()
{
    if (const size_t cached_columns = cols_data_cache.size(); cached_columns < 2)
    {
        auto columns = readFromDisk(data_provider, {(*col_defs).begin() + cached_columns, (*col_defs).begin() + 2});
        cols_data_cache.insert(cols_data_cache.end(), columns.begin(), columns.end());
    }

    return {cols_data_cache[0], cols_data_cache[1]};
}

std::pair<size_t, size_t> ColumnFileTinyReader::readRows(
    MutableColumns & output_cols,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    const size_t cached_columns = cols_data_cache.size();
    const size_t read_columns = output_cols.size();
    if (cached_columns < read_columns)
    {
        auto columns
            = readFromDisk(data_provider, {(*col_defs).begin() + cached_columns, (*col_defs).begin() + read_columns});
        cols_data_cache.insert(cols_data_cache.end(), columns.begin(), columns.end());
    }

    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Columns ColumnFileTinyReader::readFromDisk(
    const IColumnFileDataProviderPtr & data_provider,
    const std::span<const ColumnDefine> & column_defines) const
{
    const size_t num_columns_read = column_defines.size();
    Columns columns(num_columns_read); // allocate empty columns

    std::vector<size_t> fields;
    const auto & colid_to_offset = tiny_file.schema->getColIdToOffset();
    for (size_t index = 0; index < num_columns_read; ++index)
    {
        const auto & cd = column_defines[index];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_index = it->second;
            fields.emplace_back(col_index);
        }
        else
        {
            // New column after ddl is not exist in this CFTiny, fill with default value
            columns[index] = createColumnWithDefaultValue(cd, tiny_file.rows);
        }
    }

    // All columns to be read are not exist in this CFTiny and filled with default value,
    // we can skip reading from disk
    if (fields.empty())
        return columns;

    // Read the columns from disk and apply DDL cast if need
    Page page = data_provider->readTinyData(tiny_file.data_page_id, fields);
    // use `unlikely` to reduce performance impact on keyspaces without enable encryption
    if (unlikely(tiny_file.file_provider->isEncryptionEnabled(tiny_file.keyspace_id)))
    {
        // decrypt the page data in place
        size_t data_size = page.data.size();
        char * data = page.mem_holder.get();
        tiny_file.file_provider->decryptPage(tiny_file.keyspace_id, data, data_size, tiny_file.data_page_id);
    }

    for (size_t index = 0; index < num_columns_read; ++index)
    {
        // the column is filled with default values.
        if (columns[index] != nullptr)
            continue;

        const auto & cd = column_defines[index];
        auto col_index = colid_to_offset.at(cd.id);
        auto data_buf = page.getFieldData(col_index);

        // Deserialize column by pack's schema
        const auto & type = tiny_file.getDataType(cd.id);
        auto col_data = type->createColumn();
        deserializeColumn(*col_data, type, data_buf, tiny_file.rows);

        columns[index] = convertColumnByColumnDefineIfNeed(type, std::move(col_data), cd);
    }

    return columns;
}

Block ColumnFileTinyReader::readNextBlock()
{
    if (read_done)
        return {};

    auto columns = readFromDisk(data_provider, *col_defs);

    read_done = true;
    return genBlock(*col_defs, columns);
}

Block ColumnFileTinyReader::readWithFilter(const IColumn::Filter & filter)
{
    auto block = readNextBlock();
    if (size_t passed_count = countBytesInFilter(filter); passed_count != block.rows())
    {
        for (auto & col : block)
        {
            col.column = col.column->filter(filter, passed_count);
        }
    }
    return block;
}

size_t ColumnFileTinyReader::skipNextBlock()
{
    if (read_done)
        return 0;

    read_done = true;
    return tiny_file.getRows();
}

ColumnFileReaderPtr ColumnFileTinyReader::createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag)
{
    // Reuse the cache data.
    return std::make_shared<ColumnFileTinyReader>(tiny_file, data_provider, new_col_defs, cols_data_cache);
}

} // namespace DB::DM
