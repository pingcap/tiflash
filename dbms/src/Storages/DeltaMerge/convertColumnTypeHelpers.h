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

// If `from_type` is the same as `to_column_define.type`, simply return `from_col`.
// If `from_type` is different from `to_column_define.type`, check if we can apply 
// cast on read, if not, throw exception.
ColumnPtr convertColumnByColumnDefineIfNeed(const DataTypePtr & from_type, ColumnPtr && from_col, const ColumnDefine & to_column_define);

// Create a column with `num_rows`, fill with column_define.default_value or column's default.
ColumnPtr createColumnWithDefaultValue(const ColumnDefine & column_define, size_t num_rows);

} // namespace DM
} // namespace DB
