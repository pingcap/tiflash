#include <common/logger_useful.h>

#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/Delta/Pack.h>
#include <Storages/Page/PageStorage.h>

namespace DB::DM
{
using PageReadFields = PageStorage::PageReadFields;

// ================================================
// Serialize / deserialize
// ================================================

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress)
{
    CompressionMethod method = compress ? CompressionMethod::LZ4 : CompressionMethod::NONE;

    CompressedWriteBuffer compressed(buf, CompressionSettings(method));
    type->serializeBinaryBulkWithMultipleStreams(column, //
                                                 [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                 offset,
                                                 limit,
                                                 true,
                                                 {});
    compressed.next();
}

void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows)
{
    ReadBufferFromMemory buf(data_buf.begin(), data_buf.size());
    CompressedReadBuffer compressed(buf);
    type->deserializeBinaryBulkWithMultipleStreams(column, //
                                                   [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                   rows,
                                                   (double)(data_buf.size()) / rows,
                                                   true,
                                                   {});
}

inline void serializePack(const Pack & pack, const BlockPtr & schema, WriteBuffer & buf)
{
    writeIntBinary(pack.rows, buf);
    writeIntBinary(pack.bytes, buf);
    writePODBinary(pack.delete_range, buf);
    writeIntBinary(pack.data_page, buf);
    if (schema)
    {
        writeIntBinary((UInt32)schema->columns(), buf);
        for (auto & col : *pack.schema)
        {
            writeIntBinary(col.column_id, buf);
            writeStringBinary(col.name, buf);
            writeStringBinary(col.type->getName(), buf);
        }
    }
    else
    {
        writeIntBinary((UInt32)0, buf);
    }
}

inline PackPtr deserializePack(ReadBuffer & buf)
{
    auto pack   = std::make_shared<Pack>();
    pack->saved = true; // Must be true, otherwise it should not be here.
    readIntBinary(pack->rows, buf);
    readIntBinary(pack->bytes, buf);
    readPODBinary(pack->delete_range, buf);
    readIntBinary(pack->data_page, buf);
    UInt32 column_size;
    readIntBinary(column_size, buf);
    if (column_size != 0)
    {
        auto schema = std::make_shared<Block>();
        for (size_t i = 0; i < column_size; ++i)
        {
            Int64  column_id;
            String name;
            String type_name;
            readIntBinary(column_id, buf);
            readStringBinary(name, buf);
            readStringBinary(type_name, buf);
            schema->insert(ColumnWithTypeAndName({}, DataTypeFactory::instance().get(type_name), name, column_id));
        }
        pack->setSchema(schema);
    }
    return pack;
}

void serializeSavedPacks(WriteBuffer & buf, const Packs & packs)
{
    size_t saved_packs = std::find_if(packs.begin(), packs.end(), [](const PackPtr & p) { return !p->isSaved(); }) - packs.begin();

    writeIntBinary(DeltaValueSpace::CURRENT_VERSION, buf); // Add binary version
    writeIntBinary(saved_packs, buf);
    BlockPtr last_schema;

    for (auto & pack : packs)
    {
        if (!pack->isSaved())
            break;
        // Do not encode the schema if it is the same as previous one.
        if (pack->isDeleteRange())
            serializePack(*pack, nullptr, buf);
        else
        {
            if (unlikely(!pack->schema))
                throw Exception("A data pack without schema: " + pack->toString(), ErrorCodes::LOGICAL_ERROR);
            if (pack->schema != last_schema)
            {
                serializePack(*pack, pack->schema, buf);
                last_schema = pack->schema;
            }
            else
            {
                serializePack(*pack, nullptr, buf);
            }
        }
    }
}

Packs deserializePacks(ReadBuffer & buf)
{
    // Check binary version
    UInt64 version;
    readIntBinary(version, buf);
    if (version != DeltaValueSpace::CURRENT_VERSION)
        throw Exception("Pack binary version not match: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    size_t size;
    readIntBinary(size, buf);
    Packs    packs;
    BlockPtr last_schema;
    for (size_t i = 0; i < (size_t)size; ++i)
    {
        auto pack = deserializePack(buf);
        if (!pack->isDeleteRange())
        {
            if (!pack->schema)
                pack->setSchema(last_schema);
            else
                last_schema = pack->schema;
        }
        packs.push_back(pack);
    }
    return packs;
}

String packsToString(const Packs & packs)
{
    String packs_info = "[";
    for (auto & p : packs)
    {
        packs_info += (p->isDeleteRange() ? "DEL" : "INS_" + DB::toString(p->rows)) + (p->isSaved() ? "_S," : "_N,");
    }
    if (!packs.empty())
        packs_info.erase(packs_info.size() - 1);
    packs_info += "]";
    return packs_info;
}

Block readPackFromCache(const PackPtr & pack)
{
    std::scoped_lock lock(pack->cache->mutex);

    auto &         cache_block = pack->cache->block;
    MutableColumns columns     = cache_block.cloneEmptyColumns();
    for (size_t i = 0; i < cache_block.columns(); ++i)
        columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, pack->cache_offset, pack->rows);
    return cache_block.cloneWithColumns(std::move(columns));
}

Columns readPackFromCache(const PackPtr & pack, const ColumnDefines & column_defines, size_t col_start, size_t col_end)
{
    if (unlikely(!(pack->cache)))
    {
        String msg = " Not a cache pack: " + pack->toString();
        LOG_ERROR(&Logger::get(__FUNCTION__), msg);
        throw Exception(msg);
    }

    // TODO: should be able to use cache data directly, without copy.
    std::scoped_lock lock(pack->cache->mutex);

    auto &  cache_block = pack->cache->block;
    Columns columns;
    for (size_t i = col_start; i < col_end; ++i)
    {
        auto & col = column_defines[i];
        auto   it  = pack->colid_to_offset.find(col.id);
        if (it == pack->colid_to_offset.end())
        {
            // TODO: support DDL.
            throw Exception("Cannot find column with id" + DB::toString(col.id));
        }
        else
        {
            auto   col_offset = it->second;
            auto   col_data   = col.type->createColumn();
            auto & cache_col  = cache_block.getByPosition(col_offset).column;
            if (unlikely(col_offset >= cache_block.columns() || !cache_col))
            {
                String msg = "read column at " + DB::toString(col_offset)             //
                    + ", cache block: columns=" + DB::toString(cache_block.columns()) //
                    + ", rows=" + DB::toString(cache_block.rows())                    //
                    + ", read col_id: " + DB::toString(col.id)                        //
                    + ", pack: " + pack->toString();
                LOG_ERROR(&Logger::get(__FUNCTION__), msg);
                throw Exception(msg);
            }

            col_data->insertRangeFrom(*cache_col, pack->cache_offset, pack->rows);
            columns.push_back(std::move(col_data));
        }
    }
    return columns;
}

Block readPackFromDisk(const PackPtr & pack, const PageReader & page_reader)
{
    auto & schema = *pack->schema;

    PageReadFields fields;
    fields.first = pack->data_page;
    for (size_t i = 0; i < schema.columns(); ++i)
        fields.second.push_back(i);

    auto page_map = page_reader.read({fields});
    auto page     = page_map[pack->data_page];

    auto columns = schema.cloneEmptyColumns();

    if (unlikely(columns.size() != page.fieldSize()))
        throw Exception("Column size and field size not the same");

    for (size_t index = 0; index < schema.columns(); ++index)
    {
        auto   data_buf = page.getFieldData(index);
        auto & type     = schema.getByPosition(index).type;
        auto & column   = columns[index];
        deserializeColumn(*column, type, data_buf, pack->rows);
    }

    return schema.cloneWithColumns(std::move(columns));
}

Columns readPackFromDisk(const PackPtr &       pack, //
                         const PageReader &    page_reader,
                         const ColumnDefines & column_defines,
                         size_t                col_start,
                         size_t                col_end)
{
    PageReadFields fields;
    fields.first = pack->data_page;
    for (size_t index = col_start; index < col_end; ++index)
    {
        auto col_id = column_defines[index].id;
        auto it     = pack->colid_to_offset.find(col_id);
        if (it == pack->colid_to_offset.end())
            // TODO: support DDL.
            throw Exception("Cannot find column with id" + DB::toString(col_id));
        else
        {
            auto col_index = it->second;
            fields.second.push_back(col_index);
        }
    }

    auto page_map = page_reader.read({fields});
    Page page     = page_map[pack->data_page];

    Columns columns;
    for (size_t index = col_start; index < col_end; ++index)
    {
        auto col_id    = column_defines[index].id;
        auto col_index = pack->colid_to_offset[col_id];
        auto data_buf  = page.getFieldData(col_index);

        auto & cd  = column_defines[index];
        auto   col = cd.type->createColumn();
        deserializeColumn(*col, cd.type, data_buf, pack->rows);

        columns.push_back(std::move(col));
    }

    return columns;
}

} // namespace DB::DM
