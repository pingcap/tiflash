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

#include <Columns/ColumnNullable.h>


namespace DB
{
/** Replace Nullable key_columns to corresponding nested columns.
  * In 'null_map' return a map of positions where at least one column was NULL.
  * 'null_map_holder' could take ownership of null_map, if required.
  */
void extractNestedColumnsAndNullMap(
    ColumnRawPtrs & key_columns,
    ColumnPtr & null_map_holder,
    ConstNullMapPtr & null_map);

/** In 'all_key_null_map' return a map of positions where all key columns are NULL.
 *  'all_key_null_map_holder' could take ownership of null_map, if required.
 */
void extractAllKeyNullMap(
    ColumnRawPtrs & key_columns,
    ColumnPtr & all_key_null_map_holder,
    ConstNullMapPtr & all_key_null_map);

} // namespace DB
