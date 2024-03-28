// Copyright 2023 PingCAP, Inc.
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

#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/Page/Page.h>

namespace DB
{
namespace DM
{
void serializeSchema(WriteBuffer & buf, const Block & schema)
{
    if (schema)
    {
        writeIntBinary(static_cast<UInt32>(schema.columns()), buf);
        for (const auto & col : schema)
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
        schema->insert(ColumnWithTypeAndName({}, DataTypeFactory::instance().getOrSet(type_name), name, column_id));
    }
    return schema;
}

void serializeColumn(
    WriteBuffer & buf,
    const IColumn & column,
    const DataTypePtr & type,
    size_t offset,
    size_t limit,
    CompressionMethod compression_method,
    Int64 compression_level)
{
    CompressedWriteBuffer compressed(buf, CompressionSettings(compression_method, compression_level));
    type->serializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath &) { return &compressed; },
        offset,
        limit,
        true,
        {});
    compressed.next();
}

void deserializeColumn(IColumn & column, const DataTypePtr & type, std::string_view data_buf, size_t rows)
{
    ReadBufferFromString buf(data_buf);
    CompressedReadBuffer compressed(buf);
    type->deserializeBinaryBulkWithMultipleStreams(
        column,
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
        throw Exception(
            "Unexpected delta value version: " + DB::toString(STORAGE_FORMAT_CURRENT.delta),
            ErrorCodes::LOGICAL_ERROR);
    }
}

ColumnFilePersisteds deserializeSavedColumnFiles(
    const DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf)
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
        column_files = deserializeSavedColumnFilesInV2Format(context, buf, version);
        break;
    case DeltaFormat::V3:
        column_files = deserializeSavedColumnFilesInV3Format(context, segment_range, buf);
        break;
    default:
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected delta value version: {}, latest version: {}",
            version,
            DeltaFormat::V3);
    }
    return column_files;
}

ColumnFilePersisteds createColumnFilesFromCheckpoint( //
    const LoggerPtr & parent_log,
    DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs)
{
    // Check binary version
    DeltaFormat::Version version;
    readIntBinary(version, buf);

    ColumnFilePersisteds column_files;
    switch (version)
    {
    case DeltaFormat::V3:
        column_files = createColumnFilesInV3FormatFromCheckpoint(parent_log, context, segment_range, buf, temp_ps, wbs);
        break;
    default:
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected delta value version: {}, latest version: {}",
            version,
            DeltaFormat::V3);
    }
    return column_files;
}
} // namespace DM
} // namespace DB
