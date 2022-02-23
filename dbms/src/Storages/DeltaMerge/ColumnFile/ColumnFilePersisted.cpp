#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>

namespace DB
{
namespace DM
{
void serializeSchema(WriteBuffer & buf, const BlockPtr & schema)
{
    if (schema)
    {
        writeIntBinary(static_cast<UInt32>(schema->columns()), buf);
        for (auto & col : *schema)
        {
            writeIntBinary(col.column_id, buf);
            writeStringBinary(col.name, buf);
            writeStringBinary(col.type->getName(), buf);
        }
    }
    else
    {
        writeIntBinary(static_cast<UInt32>(0), buf);
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
        Int64 column_id;
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
                                                   static_cast<double>(data_buf.size()) / rows,
                                                   true,
                                                   {});
}

void serializeSavedColumnFiles(WriteBuffer & buf, const ColumnFilePersisteds & column_files)
{
    writeIntBinary(STORAGE_FORMAT_CURRENT.delta, buf); // Add binary version
    switch (STORAGE_FORMAT_CURRENT.delta)
    {
        // V1 and V2 share the same serializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        serializeSavedColumnFilesInV2Format(buf, column_files);
        break;
    case DeltaFormat::V3:
        serializeSavedColumnFilesInV3Format(buf, column_files);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(STORAGE_FORMAT_CURRENT.delta), ErrorCodes::LOGICAL_ERROR);
    }
}

ColumnFilePersisteds deserializeSavedColumnFiles(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf)
{
    // Check binary version
    DeltaFormat::Version version;
    readIntBinary(version, buf);

    ColumnFilePersisteds column_files;
    switch (version)
    {
        // V1 and V2 share the same deserializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        column_files = deserializeSavedColumnFilesInV2Format(buf, version);
        break;
    case DeltaFormat::V3:
        column_files = deserializeSavedColumnFilesInV3Format(context, segment_range, buf, version);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(version) + ", latest version: " + DB::toString(DeltaFormat::V3),
                        ErrorCodes::LOGICAL_ERROR);
    }
    return column_files;
}
} // namespace DM
} // namespace DB
