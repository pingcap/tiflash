#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{
//==========================================================================================
// Functions for casting column data when disk data type mismatch with read data type.
//==========================================================================================
ColumnPtr convertColumnByColumnDefineIfNeed(const DataTypePtr & from_type, ColumnPtr && from_col, const ColumnDefine & to_column_define);

ColumnPtr createColumnWithDefaultValue(const ColumnDefine & column_define, size_t num_rows);

void castColumnAccordingToColumnDefine(const DataTypePtr &  disk_type,
                                       const ColumnPtr &    disk_col,
                                       const ColumnDefine & read_define,
                                       MutableColumnPtr     memory_col,
                                       size_t               rows_offset,
                                       size_t               rows_limit);

} // namespace DM
} // namespace DB
