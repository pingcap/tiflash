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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>


namespace DB
{
namespace DM
{
Columns ColumnFileTiny::readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const
{
    if (!cache)
        return {};

    Columns columns;
    for (size_t i = col_start; i < col_end; ++i)
    {
        const auto & cd = column_defines[i];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_offset = it->second;
            // Copy data from cache
            const auto & type = getDataType(cd.id);
            auto col_data = type->createColumn();
            col_data->insertRangeFrom(*cache->block.getByPosition(col_offset).column, 0, rows);
            // Cast if need
            auto col_converted = convertColumnByColumnDefineIfNeed(type, std::move(col_data), cd);
            columns.push_back(std::move(col_converted));
        }
        else
        {
            ColumnPtr column = createColumnWithDefaultValue(cd, rows);
            columns.emplace_back(std::move(column));
        }
    }
    return columns;
}

Columns ColumnFileTiny::readFromDisk(const PageReader & page_reader, //
                                     const ColumnDefines & column_defines,
                                     size_t col_start,
                                     size_t col_end) const
{
    const size_t num_columns_read = col_end - col_start;

    Columns columns(num_columns_read); // allocate empty columns

    PageStorage::PageReadFields fields;
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

    auto page_map = page_reader.read({fields});
    Page page = page_map[data_page_id];
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

void ColumnFileTiny::fillColumns(const PageReader & page_reader, const ColumnDefines & col_defs, size_t col_count, Columns & result) const
{
    if (result.size() >= col_count)
        return;

    size_t col_start = result.size();
    size_t col_end = col_count;

    Columns read_cols = readFromCache(col_defs, col_start, col_end);
    if (read_cols.empty())
        read_cols = readFromDisk(page_reader, col_defs, col_start, col_end);

    result.insert(result.end(), read_cols.begin(), read_cols.end());
}

ColumnFileReaderPtr
ColumnFileTiny::getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<ColumnFileTinyReader>(*this, storage_snap, col_defs);
}

void ColumnFileTiny::serializeMetadata(WriteBuffer & buf, bool save_schema) const
{
    serializeSchema(buf, save_schema ? schema : BlockPtr{});

    writeIntBinary(data_page_id, buf);
    writeIntBinary(rows, buf);
    writeIntBinary(bytes, buf);
}

std::tuple<ColumnFilePersistedPtr, BlockPtr> ColumnFileTiny::deserializeMetadata(ReadBuffer & buf, const BlockPtr & last_schema)
{
    auto schema = deserializeSchema(buf);
    if (!schema)
        schema = last_schema;
    if (unlikely(!schema))
        throw Exception("Cannot deserialize DeltaPackBlock's schema", ErrorCodes::LOGICAL_ERROR);

    PageId data_page_id;
    size_t rows, bytes;

    readIntBinary(data_page_id, buf);
    readIntBinary(rows, buf);
    readIntBinary(bytes, buf);

    return {std::make_shared<ColumnFileTiny>(schema, rows, bytes, data_page_id), std::move(schema)};
}

Block ColumnFileTiny::readBlockForMinorCompaction(const PageReader & page_reader) const
{
    if (cache)
    {
        std::scoped_lock lock(cache->mutex);

        auto & cache_block = cache->block;
        MutableColumns columns = cache_block.cloneEmptyColumns();
        for (size_t i = 0; i < cache_block.columns(); ++i)
            columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, 0, rows);
        return cache_block.cloneWithColumns(std::move(columns));
    }
    else
    {
        const auto & schema_ref = *schema;
        auto page = page_reader.read(data_page_id);
        auto columns = schema_ref.cloneEmptyColumns();

        if (unlikely(columns.size() != page.fieldSize()))
            throw Exception("Column size and field size not the same");

        for (size_t index = 0; index < schema_ref.columns(); ++index)
        {
            auto data_buf = page.getFieldData(index);
            const auto & type = schema_ref.getByPosition(index).type;
            auto & column = columns[index];
            deserializeColumn(*column, type, data_buf, rows);
        }

        return schema_ref.cloneWithColumns(std::move(columns));
    }
}

ColumnTinyFilePtr ColumnFileTiny::writeColumnFile(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs, const BlockPtr & schema, const CachePtr & cache)
{
    auto page_id = writeColumnFileData(context, block, offset, limit, wbs);
    auto new_column_file_schema = schema ? schema : std::make_shared<Block>(block.cloneEmpty());
    auto bytes = block.bytes(offset, limit);
    return std::make_shared<ColumnFileTiny>(new_column_file_schema, limit, bytes, page_id, cache);
}

PageId ColumnFileTiny::writeColumnFileData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto page_id = context.storage_pool.newLogPageId();

    MemoryWriteBuffer write_buf;
    PageFieldSizes col_data_sizes;
    for (const auto & col : block)
    {
        auto last_buf_size = write_buf.count();
        serializeColumn(write_buf, *col.column, col.type, offset, limit, context.db_context.getSettingsRef().dt_compression_method, context.db_context.getSettingsRef().dt_compression_level);
        col_data_sizes.push_back(write_buf.count() - last_buf_size);
    }

    auto data_size = write_buf.count();
    auto buf = write_buf.tryGetReadBuffer();
    wbs.log.putPage(page_id, 0, buf, data_size, col_data_sizes);

    return page_id;
}


ColumnPtr ColumnFileTinyReader::getPKColumn()
{
    tiny_file.fillColumns(storage_snap->log_reader, *col_defs, 1, cols_data_cache);
    return cols_data_cache[0];
}

ColumnPtr ColumnFileTinyReader::getVersionColumn()
{
    tiny_file.fillColumns(storage_snap->log_reader, *col_defs, 2, cols_data_cache);
    return cols_data_cache[1];
}

size_t ColumnFileTinyReader::readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    tiny_file.fillColumns(storage_snap->log_reader, *col_defs, output_cols.size(), cols_data_cache);

    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block ColumnFileTinyReader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    tiny_file.fillColumns(storage_snap->log_reader, *col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

ColumnFileReaderPtr ColumnFileTinyReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Reuse the cache data.
    return std::make_shared<ColumnFileTinyReader>(tiny_file, storage_snap, new_col_defs, cols_data_cache);
}

} // namespace DM
} // namespace DB