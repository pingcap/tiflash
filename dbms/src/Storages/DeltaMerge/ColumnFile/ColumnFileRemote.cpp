#include <Storages/DeltaMerge/ColumnFile/ColumnFileRemote.h>

namespace DB::DM
{
ColumnFileReaderPtr
ColumnFileRemote::getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<ColumnFileRemoteReader>(*this, storage_snap, col_defs);
}
} // namespace DB::DM
