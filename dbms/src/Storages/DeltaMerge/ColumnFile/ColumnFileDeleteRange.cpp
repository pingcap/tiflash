#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace DM
{
ColumnFileReaderPtr ColumnFileDeleteRange::getReader(
    const DMContext & /*context*/,
    const StorageSnapshotPtr & /*storage_snap*/,
    const ColumnDefinesPtr & /*col_defs*/) const
{
    return std::make_shared<ColumnFileEmptyReader>();
}

void ColumnFileDeleteRange::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    delete_range.serialize(buf);
}

ColumnFilePersistedPtr ColumnFileDeleteRange::deserializeMetadata(ReadBuffer & buf)
{
    return std::make_shared<ColumnFileDeleteRange>(RowKeyRange::deserialize(buf));
}

ColumnFileReaderPtr ColumnFileEmptyReader::createNewReader(const ColumnDefinesPtr &)
{
    return std::make_shared<ColumnFileEmptyReader>();
}
} // namespace DM
} // namespace DB
