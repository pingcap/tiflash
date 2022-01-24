#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>

namespace DB
{
namespace DM
{
/// ColumnStableFile is saved on disk(include the content data and the metadata).
/// It can be recovered after reboot.
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

/// Serialize those packs' metadata into buf.
/// Note that this method stop at the first unsaved pack.
void serializeColumnStableFiles(WriteBuffer & buf, const ColumnStableFiles & column_files);
/// Recreate pack instances from buf.
ColumnStableFiles deserializeColumnStableFiles(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf);

void serializeColumnStableFiles_V2(WriteBuffer & buf, const ColumnStableFiles & column_files);
ColumnStableFiles deserializeColumnStableFiles_V2(ReadBuffer & buf, UInt64 version);

void serializeColumnStableFiles_V3(WriteBuffer & buf, const ColumnStableFiles & column_files);
ColumnStableFiles deserializeColumnStableFiles_V3(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 version);

}
}
