#pragma once

#include <Flash/Coprocessor/TiDBColumn.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
void flashColToArrowCol(TiDBColumn & dag_column, const ColumnWithTypeAndName & flash_col, const tipb::FieldType & field_type,
    size_t start_index, size_t end_index);
const char * arrowColToFlashCol(const char * pos, UInt8 field_length, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets, const ColumnWithTypeAndName & flash_col, const ColumnInfo & col_info, UInt32 length);

} // namespace DB
