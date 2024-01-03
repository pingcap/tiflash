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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>

namespace DB
{
namespace DM
{
struct ColumnFileV2
{
    UInt64 rows = 0;
    UInt64 bytes = 0;
    BlockPtr schema;
    RowKeyRange delete_range;
    PageIdU64 data_page_id = 0;

    bool isDeleteRange() const { return !delete_range.none(); }
};
using ColumnFileV2Ptr = std::shared_ptr<ColumnFileV2>;
using ColumnFileV2s = std::vector<ColumnFileV2Ptr>;

inline ColumnFilePersisteds transform_V2_to_V3(const DMContext & context, const ColumnFileV2s & column_files_v2)
{
    ColumnFilePersisteds column_files_v3;
    for (const auto & f : column_files_v2)
    {
        ColumnFilePersistedPtr f_v3;
        if (f->isDeleteRange())
            f_v3 = std::make_shared<ColumnFileDeleteRange>(std::move(f->delete_range));
        else
        {
            auto schema = getSharedBlockSchemas(context)->getOrCreate(*(f->schema));
            f_v3 = std::make_shared<ColumnFileTiny>(schema, f->rows, f->bytes, f->data_page_id, context);
        }

        column_files_v3.push_back(f_v3);
    }
    return column_files_v3;
}

inline ColumnFileV2s transformSaved_V3_to_V2(const ColumnFilePersisteds & column_files_v3)
{
    ColumnFileV2s column_files_v2;
    for (const auto & f : column_files_v3)
    {
        auto * f_v2 = new ColumnFileV2();

        if (auto * f_delete = f->tryToDeleteRange(); f_delete)
        {
            f_v2->delete_range = f_delete->getDeleteRange();
        }
        else if (auto * f_tiny_file = f->tryToTinyFile(); f_tiny_file)
        {
            f_v2->rows = f_tiny_file->getRows();
            f_v2->bytes = f_tiny_file->getBytes();
            f_v2->schema = std::make_shared<Block>(f_tiny_file->getSchema()->getSchema());
            f_v2->data_page_id = f_tiny_file->getDataPageId();
        }
        else
        {
            throw Exception("Unexpected column file type", ErrorCodes::LOGICAL_ERROR);
        }

        column_files_v2.push_back(std::shared_ptr<ColumnFileV2>(f_v2));
    }
    return column_files_v2;
}

inline void serializeColumnFile_V2(const ColumnFileV2 & column_file, const BlockPtr & schema, WriteBuffer & buf)
{
    writeIntBinary(column_file.rows, buf);
    writeIntBinary(column_file.bytes, buf);
    column_file.delete_range.serialize(buf);
    writeIntBinary(column_file.data_page_id, buf);
    if (schema)
    {
        writeIntBinary(static_cast<UInt32>(schema->columns()), buf);
        for (auto & col : *column_file.schema)
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

void serializeSavedColumnFiles_V2(WriteBuffer & buf, const ColumnFileV2s & column_files)
{
    writeIntBinary(column_files.size(), buf);
    BlockPtr last_schema;
    for (const auto & column_file : column_files)
    {
        // Do not encode the schema if it is the same as previous one.
        if (column_file->isDeleteRange())
            serializeColumnFile_V2(*column_file, nullptr, buf);
        else
        {
            if (unlikely(!column_file->schema))
                throw Exception("A data pack without schema", ErrorCodes::LOGICAL_ERROR);
            if (column_file->schema != last_schema)
            {
                serializeColumnFile_V2(*column_file, column_file->schema, buf);
                last_schema = column_file->schema;
            }
            else
            {
                serializeColumnFile_V2(*column_file, nullptr, buf);
            }
        }
    }
}

void serializeSavedColumnFilesInV2Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files)
{
    serializeSavedColumnFiles_V2(buf, transformSaved_V3_to_V2(column_files));
}

inline ColumnFileV2Ptr deserializeColumnFile_V2(ReadBuffer & buf, UInt64 version)
{
    auto column_file = std::make_shared<ColumnFileV2>();
    readIntBinary(column_file->rows, buf);
    readIntBinary(column_file->bytes, buf);
    switch (version)
    {
    case DeltaFormat::V1:
    {
        HandleRange range;
        readPODBinary(range, buf);
        column_file->delete_range = RowKeyRange::fromHandleRange(range);
        break;
    }
    case DeltaFormat::V2:
        column_file->delete_range = RowKeyRange::deserialize(buf);
        break;
    default:
        throw Exception("Unexpected version: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    }

    readIntBinary(column_file->data_page_id, buf);
    column_file->schema = deserializeSchema(buf);
    return column_file;
}

ColumnFilePersisteds deserializeSavedColumnFilesInV2Format(const DMContext & context, ReadBuffer & buf, UInt64 version)
{
    size_t size;
    readIntBinary(size, buf);
    ColumnFileV2s column_files;
    BlockPtr last_schema;
    for (size_t i = 0; i < size; ++i)
    {
        auto column_file = deserializeColumnFile_V2(buf, version);
        if (!column_file->isDeleteRange())
        {
            if (!column_file->schema)
                column_file->schema = last_schema;
            else
                last_schema = column_file->schema;
        }
        column_files.push_back(column_file);
    }
    return transform_V2_to_V3(context, column_files);
}

} // namespace DM
} // namespace DB
