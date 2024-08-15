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

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>


namespace DB
{
namespace DM
{

void serializeSavedColumnFilesInV4Format(dtpb::DeltaLayerMeta & meta, const ColumnFilePersisteds & column_files)
{
    ColumnFileSchemaPtr last_schema;

    for (const auto & column_file : column_files)
    {
        auto * cf = meta.add_files();
        switch (column_file->getType())
        {
        case ColumnFile::Type::DELETE_RANGE:
        {
            cf->set_type(dtpb::ColumnFileType::DELETE_RANGE);
            column_file->serializeMetadata(cf, false);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            cf->set_type(dtpb::ColumnFileType::BIG_FILE);
            column_file->serializeMetadata(cf, false);
            break;
        }
        case ColumnFile::Type::TINY_FILE:
        {
            cf->set_type(dtpb::ColumnFileType::TINY_FILE);
            auto * tiny_file = column_file->tryToTinyFile();
            auto cur_schema = tiny_file->getSchema();
            RUNTIME_CHECK_MSG(cur_schema, "A tiny file without schema: {}", column_file->toString());

            bool save_schema = cur_schema != last_schema;
            last_schema = cur_schema;
            column_file->serializeMetadata(cf, save_schema);
            break;
        }
        default:
            throw Exception("Unexpected type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

ColumnFilePersisteds deserializeSavedColumnFilesInV4Format(
    const DMContext & context,
    const RowKeyRange & segment_range,
    const dtpb::DeltaLayerMeta & meta)
{
    size_t column_file_count = meta.files_size();
    ColumnFilePersisteds column_files;
    column_files.reserve(column_file_count);
    ColumnFileSchemaPtr last_schema;
    for (size_t i = 0; i < column_file_count; ++i)
    {
        const auto & cf = meta.files(i);
        std::underlying_type<ColumnFile::Type>::type column_file_type = cf.type();
        ColumnFilePersistedPtr column_file;
        switch (column_file_type)
        {
        case ColumnFile::Type::DELETE_RANGE:
            column_file = ColumnFileDeleteRange::deserializeMetadata(cf.delete_range());
            break;
        case ColumnFile::Type::TINY_FILE:
        {
            column_file = ColumnFileTiny::deserializeMetadata(context, cf.tiny_file(), last_schema);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file = ColumnFileBig::deserializeMetadata(context, segment_range, cf.big_file());
            break;
        }
        default:
            throw Exception(
                "Unexpected column file type: " + DB::toString(column_file_type),
                ErrorCodes::LOGICAL_ERROR);
        }
        column_files.emplace_back(std::move(column_file));
    }
    return column_files;
}

void serializeSavedColumnFilesInV3Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files)
{
    writeIntBinary(column_files.size(), buf);
    ColumnFileSchemaPtr last_schema;

    for (const auto & column_file : column_files)
    {
        // Do not encode the schema if it is the same as previous one.
        writeIntBinary(column_file->getType(), buf);

        switch (column_file->getType())
        {
        case ColumnFile::Type::DELETE_RANGE:
        {
            column_file->serializeMetadata(buf, false);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file->serializeMetadata(buf, false);
            break;
        }
        case ColumnFile::Type::TINY_FILE:
        {
            auto * tiny_file = column_file->tryToTinyFile();
            auto cur_schema = tiny_file->getSchema();
            if (unlikely(!cur_schema))
                throw Exception("A tiny file without schema: " + column_file->toString(), ErrorCodes::LOGICAL_ERROR);

            bool save_schema = cur_schema != last_schema;
            last_schema = cur_schema;
            column_file->serializeMetadata(buf, save_schema);
            break;
        }
        default:
            throw Exception("Unexpected type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

ColumnFilePersisteds deserializeSavedColumnFilesInV3Format(
    const DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf)
{
    size_t column_file_count;
    readIntBinary(column_file_count, buf);
    ColumnFilePersisteds column_files;
    column_files.reserve(column_file_count);
    ColumnFileSchemaPtr last_schema;
    for (size_t i = 0; i < column_file_count; ++i)
    {
        std::underlying_type<ColumnFile::Type>::type column_file_type;
        readIntBinary(column_file_type, buf);
        ColumnFilePersistedPtr column_file;
        switch (column_file_type)
        {
        case ColumnFile::Type::DELETE_RANGE:
            column_file = ColumnFileDeleteRange::deserializeMetadata(buf);
            break;
        case ColumnFile::Type::TINY_FILE:
        {
            column_file = ColumnFileTiny::deserializeMetadata(context, buf, last_schema);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file = ColumnFileBig::deserializeMetadata(context, segment_range, buf);
            break;
        }
        default:
            throw Exception(
                "Unexpected column file type: " + DB::toString(column_file_type),
                ErrorCodes::LOGICAL_ERROR);
        }
        column_files.emplace_back(std::move(column_file));
    }
    return column_files;
}

ColumnFilePersisteds createColumnFilesInV3FormatFromCheckpoint( //
    const LoggerPtr & parent_log,
    DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs)
{
    size_t column_file_count;
    readIntBinary(column_file_count, buf);
    ColumnFilePersisteds column_files;
    column_files.reserve(column_file_count);
    BlockPtr last_schema;
    for (size_t i = 0; i < column_file_count; ++i)
    {
        std::underlying_type<ColumnFile::Type>::type column_file_type;
        readIntBinary(column_file_type, buf);
        ColumnFilePersistedPtr column_file;
        switch (column_file_type)
        {
        case ColumnFile::Type::DELETE_RANGE:
            column_file = ColumnFileDeleteRange::deserializeMetadata(buf);
            break;
        case ColumnFile::Type::TINY_FILE:
        {
            std::tie(column_file, last_schema)
                = ColumnFileTiny::createFromCheckpoint(parent_log, context, buf, temp_ps, last_schema, wbs);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file = ColumnFileBig::createFromCheckpoint(parent_log, context, segment_range, buf, temp_ps, wbs);
            break;
        }
        default:
            throw Exception(
                "Unexpected column file type: " + DB::toString(column_file_type),
                ErrorCodes::LOGICAL_ERROR);
        }
        column_files.emplace_back(std::move(column_file));
    }
    return column_files;
}

} // namespace DM
} // namespace DB
