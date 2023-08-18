// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

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
ColumnPtr convertColumnByColumnDefineIfNeed(
    const DataTypePtr & from_type,
    ColumnPtr && from_col,
    const ColumnDefine & to_column_define);

// Create a column with `num_rows`, fill with column_define.default_value or column's default.
ColumnPtr createColumnWithDefaultValue(const ColumnDefine & column_define, size_t num_rows);

} // namespace DM
} // namespace DB
