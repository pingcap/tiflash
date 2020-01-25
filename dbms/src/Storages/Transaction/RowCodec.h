#pragma once

#include <Storages/Transaction/DecodedRow.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>

namespace DB
{

using TiDB::ColumnInfo;
using TiDB::TableInfo;

/// TiDB row encode/decode, handles both row format V1/V2, and respects schema (outer should take care of schema mismatching cases).
DecodedRow * decodeRow(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut);

/// Decode an unknown column's raw value (as string) in a V2 row format, based on the given column info.
/// It is used for force decoding when the column info is certain, before which pre-decoding hasn't seen its column info yet thus can only record its raw value.
Field decodeUnknownColumnV2(Field & unknown, const ColumnInfo & column_info);

/// The following two encode functions are used for testing.
void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, std::stringstream & ss);
void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, std::stringstream & ss);

} // namespace DB
