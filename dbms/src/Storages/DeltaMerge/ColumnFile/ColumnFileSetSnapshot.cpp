#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace DM
{
RowKeyRange ColumnFileSetSnapshot::getSquashDeleteRange() const
{
    RowKeyRange squashed_delete_range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
    for (auto iter = column_files.cbegin(); iter != column_files.cend(); ++iter)
    {
        const auto & column_file = *iter;
        if (auto f_delete = column_file->tryToDeleteRange(); f_delete)
            squashed_delete_range = squashed_delete_range.merge(f_delete->getDeleteRange());
    }
    return squashed_delete_range;
}
} // namespace DM
} // namespace DB