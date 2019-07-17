#include <Storages/DeltaMerge/Chunk.h>

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

        if (chunk.rows && chunk.rows != d.rows)
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

void readChunkData(MutableColumns &      columns,
                   const Chunk &         chunk,
                   const ColumnDefines & column_defines,
                   PageStorage &         storage,
                   size_t                rows_offset,
                   size_t                rows_limit)
{
    std::unordered_map<PageId, size_t> page_to_index;
    PageIds                            page_ids;
    page_ids.reserve(column_defines.size());
    for (size_t index = 0; index < column_defines.size(); ++index)
    {
        auto & define  = column_defines[index];
        auto   page_id = chunk.getColumn(define.id).page_id;
        page_ids.push_back(page_id);
        page_to_index[page_id] = index;
    }

    PageHandler page_handler = [&](PageId page_id, const Page & page) {
        size_t index = page_to_index[page_id];

        ColumnDefine         define = column_defines[index];
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        const ColumnMeta &   meta = chunk.getColumn(define.id);
        IColumn &            col  = *columns[index];

        if (!rows_offset)
        {
            deserializeColumn(col, meta, page, rows_limit);
        }
        else
        {
            auto tmp_col = define.type->createColumn();
            deserializeColumn(*tmp_col, meta, page, rows_offset + rows_limit);
            col.insertRangeFrom(*tmp_col, rows_offset, rows_limit);
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
        ColumnDefine          define = read_column_defines[index];
        ColumnWithTypeAndName col;
        col.type      = define.type;
        col.name      = define.name;
        col.column_id = define.id;
        col.column    = std::move(columns[index]);

        res.insert(col);
    }
    return res;
}

} // namespace DM
} // namespace DB