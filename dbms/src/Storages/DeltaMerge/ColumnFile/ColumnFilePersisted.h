#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>

namespace DB
{
namespace DM
{
class ColumnFilePersisted;
using ColumnFilePersistedPtr = std::shared_ptr<ColumnFilePersisted>;
using ColumnFilePersisteds = std::vector<ColumnFilePersistedPtr>;

// TODO: move `removeData` and `serializeMetadata` into this class
// represents ColumnFile that can be saved on disk
class ColumnFilePersisted : public ColumnFile
{
};

void serializeSchema(WriteBuffer & buf, const BlockPtr & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows);

/// Serialize those packs' metadata into buf.
/// Note that this method stop at the first unsaved pack.
void serializeSavedColumnFiles(WriteBuffer & buf, const ColumnFiles & column_files);
/// Recreate pack instances from buf.
ColumnFiles deserializeSavedColumnFiles(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf);

void serializeSavedColumnFilesInV2Format(WriteBuffer & buf, const ColumnFiles & column_files);
ColumnFiles deserializeSavedColumnFilesInV2Format(ReadBuffer & buf, UInt64 version);

void serializeSavedColumnFilesInV3Format(WriteBuffer & buf, const ColumnFiles & column_files);
ColumnFiles deserializeSavedColumnFilesInV3Format(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 version);

} // namespace DM
} // namespace DB
