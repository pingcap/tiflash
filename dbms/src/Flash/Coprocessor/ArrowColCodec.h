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

#include <Flash/Coprocessor/TiDBColumn.h>

namespace DB
{
void flashColToArrowCol(
    TiDBColumn & dag_column,
    const ColumnWithTypeAndName & flash_col,
    const tipb::FieldType & field_type,
    size_t start_index,
    size_t end_index);
const char * arrowColToFlashCol(
    const char * pos,
    UInt8 field_length,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets,
    const ColumnWithTypeAndName & flash_col,
    const TiDB::ColumnInfo & col_info,
    UInt32 length);

} // namespace DB
