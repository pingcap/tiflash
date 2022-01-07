#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/ColumnDeleteRangeFile.h>


namespace DB
{
namespace DM
{
ColumnFileReaderPtr ColumnDeleteRangeFile::getReader(const DB::DM::DMContext &, const DB::DM::StorageSnapshotPtr &, const DB::DM::ColumnDefinesPtr &) const
{
    return std::make_shared<ColumnFileEmptyReader>();
}

void ColumnDeleteRangeFile::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    delete_range.serialize(buf);
}

ColumnFileReaderPtr ColumnFileEmptyReader::createNewReader(const ColumnDefinesPtr &)
{
    return std::make_shared<ColumnFileEmptyReader>();
}
}
}
