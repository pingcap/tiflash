#include <Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/DMContext.h>


namespace DB
{
namespace DM
{
ColumnFileReaderPtr ColumnDeleteRangeFile::getReader(
    const DMContext & /*context*/,
    const StorageSnapshotPtr & /*storage_snap*/,
    const ColumnDefinesPtr & /*col_defs*/) const
{
    return std::make_shared<ColumnFileEmptyReader>();
}

void ColumnDeleteRangeFile::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    delete_range.serialize(buf);
}

ColumnStableFilePtr ColumnDeleteRangeFile::deserializeMetadata(ReadBuffer & buf)
{
    return std::make_shared<ColumnDeleteRangeFile>(RowKeyRange::deserialize(buf));
}

ColumnFileReaderPtr ColumnFileEmptyReader::createNewReader(const ColumnDefinesPtr &)
{
    return std::make_shared<ColumnFileEmptyReader>();
}
}
}
