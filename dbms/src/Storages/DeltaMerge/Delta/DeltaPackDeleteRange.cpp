#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>

namespace DB
{
namespace DM
{

DeltaPackReaderPtr DeltaPackDeleteRange::getReader(const DMContext & /*context*/,
                                                   const StorageSnapshotPtr & /*storage_snap*/,
                                                   const ColumnDefinesPtr & /*col_defs*/) const
{
    return std::make_shared<DPEmptyReader>();
}

void DeltaPackDeleteRange::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    delete_range.serialize(buf);
}

DeltaPackPtr DeltaPackDeleteRange::deserializeMetadata(ReadBuffer & buf)
{
    return std::make_shared<DeltaPackDeleteRange>(RowKeyRange::deserialize(buf));
}

DeltaPackReaderPtr DPEmptyReader::createNewReader(const ColumnDefinesPtr &)
{
    return std::make_shared<DPEmptyReader>();
}

} // namespace DM
} // namespace DB