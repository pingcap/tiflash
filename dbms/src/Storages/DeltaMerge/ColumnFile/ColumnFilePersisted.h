#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>

namespace DB
{
namespace DM
{
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
};

void serializeSchema(WriteBuffer & buf, const BlockPtr & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows);

/// Serialize those column files' metadata into buf.
void serializeSavedColumnFiles(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
/// Recreate column file instances from buf.
ColumnFilePersisteds deserializeSavedColumnFiles(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf);

void serializeSavedColumnFilesInV2Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
ColumnFilePersisteds deserializeSavedColumnFilesInV2Format(ReadBuffer & buf, UInt64 version);

void serializeSavedColumnFilesInV3Format(WriteBuffer & buf, const ColumnFilePersisteds & column_files);
ColumnFilePersisteds deserializeSavedColumnFilesInV3Format(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 version);

} // namespace DM
} // namespace DB
