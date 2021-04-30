#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
}

namespace DB
{
namespace DM
{

DeltaPackPtr DeltaPackBlock::writePack(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto page_id = writePackData(context, block, offset, limit, wbs);
    auto schema  = std::make_shared<Block>(block.cloneEmpty());
    auto bytes   = block.bytes(offset, limit);
    return createPackWithDataPage(schema, limit, bytes, page_id);
}

DeltaPackPtr DeltaPackBlock::createPackWithDataPage(const BlockPtr & schema, UInt64 rows, UInt64 bytes, PageId data_page_id)
{
    auto * p          = new DeltaPackBlock(schema);
    p->rows           = rows;
    p->bytes          = bytes;
    p->data_page_id   = data_page_id;
    p->disable_append = true;
    return DeltaPackPtr(p);
}

DeltaPackPtr DeltaPackBlock::createCachePack(const BlockPtr & schema)
{
    auto * p = new DeltaPackBlock(schema);
    p->cache = std::make_shared<Cache>(*schema);
    return DeltaPackPtr(p);
}

void DeltaPackBlock::appendToCache(const Block data, size_t offset, size_t limit, size_t data_bytes)
{
    std::scoped_lock cache_lock(cache->mutex);

    for (size_t i = 0; i < cache->block.columns(); ++i)
    {
        auto & col               = data.getByPosition(i).column;
        auto & cache_col         = *cache->block.getByPosition(i).column;
        auto * mutable_cache_col = const_cast<IColumn *>(&cache_col);
        mutable_cache_col->insertRangeFrom(*col, offset, limit);
    }

    rows += limit;
    bytes += data_bytes;
}

void DeltaPackBlock::fillColumns(const PageReader & page_reader, const ColumnDefines & col_defs, size_t col_count, Columns & result) const
{
    if (result.size() >= col_count)
        return;

    size_t col_start = result.size();
    size_t col_end   = col_count;

    Columns read_cols;
    if (isCached())
        read_cols = readFromCache(col_defs, col_start, col_end);
    else if (getDataPageId() != 0)
        read_cols = readFromDisk(page_reader, col_defs, col_start, col_end);
    else
        throw Exception("Pack is in illegal status: " + toString(), ErrorCodes::LOGICAL_ERROR);

    result.insert(result.end(), read_cols.begin(), read_cols.end());
}

PageId DeltaPackBlock::writePackData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto page_id = context.storage_pool.newLogPageId();

    MemoryWriteBuffer write_buf;
    PageFieldSizes    col_data_sizes;
    for (auto & col : block)
    {
        auto last_buf_size = write_buf.count();
        serializeColumn(write_buf, *col.column, col.type, offset, limit, true);
        col_data_sizes.push_back(write_buf.count() - last_buf_size);
    }

    auto data_size = write_buf.count();
    auto buf       = write_buf.tryGetReadBuffer();
    wbs.log.putPage(page_id, 0, buf, data_size, col_data_sizes);

    ProfileEvents::increment(ProfileEvents::DMWriteBytes, data_size);

    return page_id;
}

Block DeltaPackBlock::readFromCache() const
{
    std::scoped_lock lock(cache->mutex);

    auto &         cache_block = cache->block;
    MutableColumns columns     = cache_block.cloneEmptyColumns();
    for (size_t i = 0; i < cache_block.columns(); ++i)
        columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, 0, rows);
    return cache_block.cloneWithColumns(std::move(columns));
}

Block DeltaPackBlock::readFromDisk(const PageReader & page_reader) const
{
    auto & schema_ = *schema;

    PageStorage::PageReadFields fields;
    fields.first = data_page_id;
    for (size_t i = 0; i < schema_.columns(); ++i)
        fields.second.push_back(i);

    auto page_map = page_reader.read({fields});
    auto page     = page_map[data_page_id];

    auto columns = schema_.cloneEmptyColumns();

    if (unlikely(columns.size() != page.fieldSize()))
        throw Exception("Column size and field size not the same");

    for (size_t index = 0; index < schema_.columns(); ++index)
    {
        auto   data_buf = page.getFieldData(index);
        auto & type     = schema_.getByPosition(index).type;
        auto & column   = columns[index];
        deserializeColumn(*column, type, data_buf, rows);
    }

    return schema_.cloneWithColumns(std::move(columns));
}

Columns DeltaPackBlock::readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const
{
    if (unlikely(!(cache)))
    {
        String msg = " Not a cache pack: " + toString();
        LOG_ERROR(&Logger::get(__FUNCTION__), msg);
        throw Exception(msg);
    }

    std::scoped_lock lock(cache->mutex);

    const auto & cache_block = cache->block;
    Columns      columns;
    for (size_t i = col_start; i < col_end; ++i)
    {
        const auto & cd = column_defines[i];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_offset = it->second;
            // Copy data from cache
            auto [type, col_data] = getDataTypeAndEmptyColumn(cd.id);
            col_data->insertRangeFrom(*cache_block.getByPosition(col_offset).column, 0, rows);
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

Columns DeltaPackBlock::readFromDisk(const PageReader &    page_reader, //
                                     const ColumnDefines & column_defines,
                                     size_t                col_start,
                                     size_t                col_end) const
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
    Page page     = page_map[data_page_id];
    for (size_t index = col_start; index < col_end; ++index)
    {
        const size_t index_in_read_columns = index - col_start;
        if (columns[index_in_read_columns] != nullptr)
        {
            // the column is fill with default values.
            continue;
        }
        auto col_id    = column_defines[index].id;
        auto col_index = colid_to_offset.at(col_id);
        auto data_buf  = page.getFieldData(col_index);

        const auto & cd = column_defines[index];
        // Deserialize column by pack's schema
        auto [type, col_data] = getDataTypeAndEmptyColumn(cd.id);
        deserializeColumn(*col_data, type, data_buf, rows);

        columns[index_in_read_columns] = convertColumnByColumnDefineIfNeed(type, std::move(col_data), cd);
    }

    return columns;
}


DeltaPackReaderPtr
DeltaPackBlock::getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<DPBlockReader>(*this, storage_snap, col_defs);
}

void DeltaPackBlock::serializeMetadata(WriteBuffer & buf, bool save_schema) const
{
    serializeSchema(buf, save_schema ? schema : BlockPtr{});

    writeIntBinary(data_page_id, buf);
    writeIntBinary(rows, buf);
    writeIntBinary(bytes, buf);
}

std::tuple<DeltaPackPtr, BlockPtr> DeltaPackBlock::deserializeMetadata(ReadBuffer & buf, const BlockPtr & last_schema)
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

    return {createPackWithDataPage(schema, rows, bytes, data_page_id), std::move(schema)};
}

ColumnPtr DPBlockReader::getPKColumn()
{
    pack.fillColumns(storage_snap->log_reader, *col_defs, 1, cols_data_cache);
    return cols_data_cache[0];
}

ColumnPtr DPBlockReader::getVersionColumn()
{
    pack.fillColumns(storage_snap->log_reader, *col_defs, 2, cols_data_cache);
    return cols_data_cache[1];
}

size_t DPBlockReader::readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    pack.fillColumns(storage_snap->log_reader, *col_defs, output_cols.size(), cols_data_cache);

    auto pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block DPBlockReader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    pack.fillColumns(storage_snap->log_reader, *col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

DeltaPackReaderPtr DPBlockReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Reuse the cache data.
    return std::make_shared<DPBlockReader>(pack, storage_snap, new_col_defs, cols_data_cache);
}


} // namespace DM
} // namespace DB
