#pragma once

#include <Storages/DeltaMerge/ColumnFile.h>

namespace DB
{
namespace DM
{
class ColumnStableFile : public ColumnFile
{
public:
    // distinguish metadata between different kinds of `ColumnFile`
    enum Type : UInt32
    {
        DELETE_RANGE = 1,
        TINY_FILE = 2,
        BIG_FILE = 3,
    };

    virtual Type getType() const = 0;
    /// Put the data's page id into the corresponding WriteBatch.
    /// The actual remove will be done later.
    virtual void removeData(WriteBatches &) const = 0;

    virtual void serializeMetadata(WriteBuffer & buf, bool save_schema) const = 0;
};

void serializeSchema(WriteBuffer & buf, const BlockPtr & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows);
}
}
