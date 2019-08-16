#include <Storages/DeltaMerge/Chunk.h>

#include <DataTypes/isLossyCast.h>
#include <Functions/FunctionHelpers.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>

namespace DB
{
namespace DM
{
void Chunk::serialize(WriteBuffer & buf) const
{
    writeIntBinary(handle_start, buf);
    writeIntBinary(handle_end, buf);
    writePODBinary(is_delete_range, buf);
    writeIntBinary((UInt64)columns.size(), buf);
    for (const auto & [col_id, d] : columns)
    {
        writeIntBinary(col_id, buf);
        writeIntBinary(d.page_id, buf);
        writeIntBinary(d.rows, buf);
        writeIntBinary(d.bytes, buf);
        writeStringBinary(d.type->getName(), buf);
    }
}

Chunk Chunk::deserialize(ReadBuffer & buf)
{
    Handle start, end;
    readIntBinary(start, buf);
    readIntBinary(end, buf);

    Chunk chunk(start, end);

    readPODBinary(chunk.is_delete_range, buf);
    UInt64 col_size;
    readIntBinary(col_size, buf);
    chunk.columns.reserve(col_size);
    for (UInt64 ci = 0; ci < col_size; ++ci)
    {
        ColumnMeta d;
        String     type;
        readIntBinary(d.col_id, buf);
        readIntBinary(d.page_id, buf);
        readIntBinary(d.rows, buf);
        readIntBinary(d.bytes, buf);
        readStringBinary(type, buf);

        d.type = DataTypeFactory::instance().get(type);

        chunk.columns.emplace(d.col_id, d);

        if (chunk.rows != 0 && chunk.rows != d.rows)
            throw Exception("Rows not match");
        else
            chunk.rows = d.rows;
    }
    return chunk;
}

void serializeChunks(
    WriteBuffer & buf, Chunks::const_iterator begin, Chunks ::const_iterator end, const Chunk * extra1, const Chunk * extra2)
{
    auto size = (UInt64)(end - begin);
    if (extra1)
        ++size;
    if (extra2)
        ++size;
    writeIntBinary(size, buf);

    for (; begin != end; ++begin)
        (*begin).serialize(buf);
    if (extra1)
        extra1->serialize(buf);
    if (extra2)
        extra2->serialize(buf);
}

Chunks deserializeChunks(ReadBuffer & buf)
{
    Chunks chunks;
    UInt64 size;
    readIntBinary(size, buf);
    for (UInt64 i = 0; i < size; ++i)
        chunks.push_back(Chunk::deserialize(buf));
    return chunks;
}

using BufferAndSize = std::pair<ReadBufferPtr, size_t>;
BufferAndSize serializeColumn(const IColumn & column, const DataTypePtr & type, size_t offset, size_t num, bool compress)
{
    MemoryWriteBuffer plain;
    CompressionMethod method = compress ? CompressionMethod::LZ4 : CompressionMethod::NONE;

    CompressedWriteBuffer compressed(plain, CompressionSettings(method));
    type->serializeBinaryBulkWithMultipleStreams(column, //
                                                 [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                 offset,
                                                 num,
                                                 true,
                                                 {});
    compressed.next();

    auto data_size = plain.count();
    return {plain.tryGetReadBuffer(), data_size};
}

Chunk prepareChunkDataWrite(const DMContext & dm_context, const GenPageId & gen_data_page_id, WriteBatch & wb, const Block & block)
{
    auto & handle_col_data = getColumnVectorData<Handle>(block, block.getPositionByName(dm_context.table_handle_define.name));
    Chunk  chunk(handle_col_data[0], handle_col_data[handle_col_data.size() - 1]);
    for (const auto & col_define : dm_context.table_columns)
    {
        auto            col_id = col_define.id;
        const IColumn & column = *(block.getByName(col_define.name).column);
        auto [buf, size]       = serializeColumn(column, col_define.type, 0, column.size(), !dm_context.not_compress.count(col_id));

        ColumnMeta d;
        d.col_id  = col_id;
        d.page_id = gen_data_page_id();
        d.rows    = column.size();
        d.bytes   = size;
        d.type    = col_define.type;

        wb.putPage(d.page_id, 0, buf, size);
        chunk.insert(d);
    }

    return chunk;
}

void deserializeColumn(IColumn & column, const ColumnMeta & meta, const Page & page, size_t rows_limit)
{
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    CompressedReadBuffer compressed(buf);
    meta.type->deserializeBinaryBulkWithMultipleStreams(column, //
                                                        [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                        rows_limit,
                                                        (double)(page.data.size()) / meta.rows,
                                                        true,
                                                        {});
}

void columnTypeCast(const Page &         page,
                    const ColumnDefine & read_define,
                    const ColumnMeta &   disk_meta,
                    MutableColumnPtr     col,
                    size_t               rows_offset,
                    size_t               rows_limit)
{
    // sanity check
    if (unlikely(!isSupportedDataTypeCast(disk_meta.type, read_define.type)))
    {
        throw Exception("Reading mismatch data type chunk. Cast from " + disk_meta.type->getName() + " to " + read_define.type->getName()
                            + " is NOT supported!",
                        ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    // read from disk according as chunk meta
    MutableColumnPtr tmp_col = disk_meta.type->createColumn();
    deserializeColumn(*tmp_col, disk_meta, page, rows_offset + rows_limit);

    // cast to current DataType
#if 1
    // TODO this is awful, can we copy memory by using something like static_cast<> ?
    for (size_t i = 0; i < tmp_col->size(); ++i)
    {
        Field f = (*tmp_col)[i];
        if (f.getType() == Field::Types::Null)
            col->insertDefault(); // TODO
        else
            col->insert(std::move(f));
    }

#ifndef NDEBUG
    LOG_TRACE(&Poco::Logger::get("Chunk"),
              "Read " + DB::toString(tmp_col->size()) + " rows from page with off:lim=" + DB::toString(rows_offset) + ":"
                  + DB::toString(rows_limit));
#endif
#else
    auto & memory_array = typeid_cast<ColumnVector<Int32> &>(col).getData();
    auto & disk_data    = typeid_cast<ColumnVector<Int8> &>(*tmp_col).getData();
    for (size_t i = 0; i < rows_limit; ++i)
    {
        // implicit cast from disk_meta.type to read_define.type
        memory_array[i + rows_offset] = disk_data[i];
    }
#endif
}

void readChunkData(MutableColumns &      columns,
                   const Chunk &         chunk,
                   const ColumnDefines & column_defines,
                   PageStorage &         storage,
                   size_t                rows_offset,
                   size_t                rows_limit)
{
    assert(!chunk.isDeleteRange());
    // Caller should already allocate memory for each column in columns

    std::unordered_map<PageId, size_t> page_to_index;
    PageIds                            page_ids;
    page_ids.reserve(column_defines.size());
    for (size_t index = 0; index < column_defines.size(); ++index)
    {
        const auto & define = column_defines[index];
        if (chunk.hasColumn(define.id))
        {
            // read chunk's data from PageStorage later
            auto page_id = chunk.getColumn(define.id).page_id;
            page_ids.push_back(page_id);
            page_to_index[page_id] = index;
        }
        else
        {
            // new column is not exist in chunk's meta, fill with default value
            IColumn & col = *columns[index];
            // TODO this is awful
            for (size_t row_index = 0; row_index < rows_limit; ++row_index)
            {
                if (define.default_value.empty())
                {
                    col.insertDefault();
                }
                else
                {
                    ReadBufferFromMemory buf(define.default_value.c_str(), define.default_value.size());
                    define.type->deserializeTextEscaped(col, buf);
                }
            }
        }
    }

    PageHandler page_handler = [&](PageId page_id, const Page & page) {
        size_t               index       = page_to_index[page_id];
        IColumn &            col         = *columns[index];
        const ColumnDefine & read_define = column_defines[index];
        const ColumnMeta &   disk_meta   = chunk.getColumn(read_define.id);

        // define.type is current type at memory
        // meta.type is the type at disk (maybe different from define.type)

        if (read_define.type->equals(*disk_meta.type))
        {
#ifndef NDEBUG
            const auto && [first, last] = chunk.getHandleFirstLast();
            const String disk_col       = "col{name:" + DB::toString(read_define.name) + ",id:" + DB::toString(disk_meta.col_id) + ",type"
                                          + disk_meta.type->getName() + "]";
            LOG_TRACE(&Poco::Logger::get("Chunk"),
                      "Reading chunk[" + DB::toString(first) + "-" + DB::toString(last) + "] " + disk_col);
#endif
            if (rows_offset == 0)
            {
                deserializeColumn(col, disk_meta, page, rows_limit);
            }
            else
            {
                MutableColumnPtr tmp_col = read_define.type->createColumn();
                deserializeColumn(*tmp_col, disk_meta, page, rows_offset + rows_limit);
                col.insertRangeFrom(*tmp_col, rows_offset, rows_limit);
            }
        }
        else
        {
#ifndef NDEBUG
            const auto && [first, last] = chunk.getHandleFirstLast();
            const String disk_col       = "col{name:" + DB::toString(read_define.name) + ",id:" + DB::toString(disk_meta.col_id) + ",type"
                + disk_meta.type->getName() + "]";
            LOG_TRACE(&Poco::Logger::get("Chunk"),
                      "Reading chunk[" + DB::toString(first) + "-" + DB::toString(last) + "] " + disk_col + " as type "
                          + read_define.type->getName());
#endif

            columnTypeCast(page, read_define, disk_meta, col.getPtr(), rows_offset, rows_limit);
        }
    };
    storage.read(page_ids, page_handler);
}


Block readChunk(const Chunk & chunk, const ColumnDefines & read_column_defines, PageStorage & data_storage)
{
    if (read_column_defines.empty())
        return {};

    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(chunk.getRows());
    }

    if (chunk.getRows())
    {
        // Read from storage
        readChunkData(columns, chunk, read_column_defines, data_storage, 0, chunk.getRows());
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        ColumnWithTypeAndName col;
        const ColumnDefine &  define = read_column_defines[index];
        col.type                     = define.type;
        col.name                     = define.name;
        col.column_id                = define.id;
        col.column                   = std::move(columns[index]);

        res.insert(std::move(col));
    }
    return res;
}

} // namespace DM
} // namespace DB