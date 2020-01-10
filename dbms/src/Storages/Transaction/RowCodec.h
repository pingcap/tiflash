#pragma once

#include <Storages/Transaction/DecodedRow.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>

namespace DB
{

using TiDB::TableInfo;

/// TiDB row encode/decode, handles both row format V1/V2, and respects schema (outer should take care of schema mismatching cases).
DecodedRow * decodeRow(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut);

} // namespace DB
