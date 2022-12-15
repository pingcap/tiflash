#include <Storages/DeltaMerge/ColumnFile/ColumnFileRemote.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>

namespace DB::DM
{

ColumnFileReaderPtr ColumnFileRemote::getReader(
    const DMContext & /*context*/,
    const IColumnFileSetStorageReaderPtr & reader,
    const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<ColumnFileRemoteReader>(*this, reader, col_defs);
}

} // namespace DB::DM
