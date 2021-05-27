#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/Delta/DeltaPack.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>

namespace DB
{
namespace DM
{

/// ======================================================
/// Helper methods.
/// ======================================================

size_t copyColumnsData(
    const Columns & from, const ColumnPtr & pk_col, MutableColumns & to, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    if (range)
    {
        RowKeyColumnContainer rkcc(pk_col, range->is_common_handle);
        if (rows_limit == 1)
        {
            if (range->check(rkcc.getRowKeyValue(rows_offset)))
            {
                for (size_t col_index = 0; col_index < to.size(); ++col_index)
                    to[col_index]->insertFrom(*from[col_index], rows_offset);
                return 1;
            }
            else
            {
                return 0;
            }
        }
        else
        {
            auto [actual_offset, actual_limit] = RowKeyFilter::getPosRangeOfSorted(*range, pk_col, rows_offset, rows_limit);
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], actual_offset, actual_limit);
            return actual_limit;
        }
    }
    else
    {
        if (rows_limit == 1)
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertFrom(*from[col_index], rows_offset);
        }
        else
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], rows_offset, rows_limit);
        }
        return rows_limit;
    }
}

void serializeSchema(WriteBuffer & buf, const BlockPtr & schema)
{
    if (schema)
    {
        writeIntBinary((UInt32)schema->columns(), buf);
        for (auto & col : *schema)
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

BlockPtr deserializeSchema(ReadBuffer & buf)
{
    UInt32 cols;
    readIntBinary(cols, buf);
    if (!cols)
        return {};
    auto schema = std::make_shared<Block>();
    for (size_t i = 0; i < cols; ++i)
    {
        Int64  column_id;
        String name;
        String type_name;
        readIntBinary(column_id, buf);
        readStringBinary(name, buf);
        readStringBinary(type_name, buf);
        schema->insert(ColumnWithTypeAndName({}, DataTypeFactory::instance().get(type_name), name, column_id));
    }
    return schema;
}

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


DeltaPackBlock * DeltaPack::tryToBlock()
{
    return !isBlock() ? nullptr : static_cast<DeltaPackBlock *>(this);
}

DeltaPackFile * DeltaPack::tryToFile()
{
    return !isFile() ? nullptr : static_cast<DeltaPackFile *>(this);
}

DeltaPackDeleteRange * DeltaPack::tryToDeleteRange()
{
    return !isDeleteRange() ? nullptr : static_cast<DeltaPackDeleteRange *>(this);
}

void serializeSavedPacks(WriteBuffer & buf, const DeltaPacks & packs)
{
    writeIntBinary(STORAGE_FORMAT_CURRENT.delta, buf); // Add binary version
    switch (STORAGE_FORMAT_CURRENT.delta)
    {
        // V1 and V2 share the same serializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        serializeSavedPacks_v2(buf, packs);
        break;
    case DeltaFormat::V3:
        serializeSavedPacks_V3(buf, packs);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(STORAGE_FORMAT_CURRENT.delta), ErrorCodes::LOGICAL_ERROR);
    }
}

DeltaPacks deserializePacks(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf)
{
    // Check binary version
    DeltaFormat::Version version;
    readIntBinary(version, buf);

    DeltaPacks packs;
    switch (version)
    {
    // V1 and V2 share the same deserializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        packs = deserializePacks_V2(buf, version);
        break;
    case DeltaFormat::V3:
        packs = deserializePacks_V3(context, segment_range, buf, version);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(version) + ", latest version: " + DB::toString(DeltaFormat::V3),
                        ErrorCodes::LOGICAL_ERROR);
    }
    for (auto & p : packs)
        p->setSaved();
    return packs;
}


String packsToString(const DeltaPacks & packs)
{
    String packs_info = "[";
    for (auto & p : packs)
    {
        if (p->isBlock())
            packs_info += "B_" + DB::toString(p->getRows());
        else if (p->isFile())
            packs_info += "F_" + DB::toString(p->getRows());
        else if (auto dp_delete = p->tryToDeleteRange(); dp_delete)
            packs_info += "D_" + dp_delete->getDeleteRange().toString();
        packs_info += (p->isSaved() ? "_S," : "_N,");
    }
    if (!packs.empty())
        packs_info.erase(packs_info.size() - 1);
    packs_info += "]";
    return packs_info;
}

} // namespace DM
} // namespace DB
