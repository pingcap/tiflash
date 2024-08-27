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

#pragma once

#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <IO/Compression/CompressionMethod.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/dtpb/column_file.pb.h>

namespace DB
{
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;
namespace DM
{
struct WriteBatches;
class ColumnFilePersisted;
using ColumnFilePersistedPtr = std::shared_ptr<ColumnFilePersisted>;
using ColumnFilePersisteds = std::vector<ColumnFilePersistedPtr>;

// represents ColumnFile that can be saved on disk
class ColumnFilePersisted : public ColumnFile
{
public:
    /// Put the data's page id into the corresponding WriteBatch.
    /// The actual remove will be done later.
    virtual void removeData(WriteBatches &) const {};

    virtual void serializeMetadata(WriteBuffer & buf, bool save_schema) const = 0;

    virtual void serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool save_schema) const = 0;
};

void serializeSchema(WriteBuffer & buf, const Block & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);
void serializeSchema(dtpb::ColumnFileTiny * tiny_pb, const Block & schema);
BlockPtr deserializeSchema(const ::google::protobuf::RepeatedPtrField<::dtpb::ColumnSchema> & schema_pb);

void serializeColumn(
    WriteBuffer & buf,
    const IColumn & column,
    const DataTypePtr & type,
    size_t offset,
    size_t limit,
    CompressionMethod compression_method,
    Int64 compression_level);
void deserializeColumn(IColumn & column, const DataTypePtr & type, std::string_view data_buf, size_t rows);

/// Serialize those column files' metadata into buf.
void serializeSavedColumnFiles(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
/// Recreate column file instances from buf.
ColumnFilePersisteds deserializeSavedColumnFiles(
    const DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf);

ColumnFilePersisteds createColumnFilesFromCheckpoint( //
    const LoggerPtr & parent_log,
    DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs);

void serializeSavedColumnFilesInV2Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
ColumnFilePersisteds deserializeSavedColumnFilesInV2Format(const DMContext & context, ReadBuffer & buf, UInt64 version);

void serializeSavedColumnFilesInV3Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
ColumnFilePersisteds deserializeSavedColumnFilesInV3Format(
    const DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf);

ColumnFilePersisteds createColumnFilesInV3FormatFromCheckpoint(
    const LoggerPtr & parent_log,
    DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs);

void serializeSavedColumnFilesInV4Format(dtpb::DeltaLayerMeta & meta, const ColumnFilePersisteds & column_files);
ColumnFilePersisteds deserializeSavedColumnFilesInV4Format(
    const DMContext & context,
    const RowKeyRange & segment_range,
    const dtpb::DeltaLayerMeta & meta);

ColumnFilePersisteds createColumnFilesInV4FormatFromCheckpoint(
    const LoggerPtr & parent_log,
    DMContext & context,
    const RowKeyRange & segment_range,
    const dtpb::DeltaLayerMeta & meta,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs);
} // namespace DM
} // namespace DB
