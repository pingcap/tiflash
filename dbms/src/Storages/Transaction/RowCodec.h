#pragma once

#include <Storages/Transaction/DecodedRow.h>
#include <Storages/Transaction/TiKVKeyValue.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

using TiDB::ColumnInfo;
using TiDB::TableInfo;

using ColumnIdToIndex = google::dense_hash_map<ColumnID, size_t>;
constexpr ColumnID EmptyColumnID = TiDBPkColumnID - 1;
constexpr ColumnID DeleteColumnID = EmptyColumnID - 1;

// should keep the same way tidb does.
Field GenDefaultField(const TiDB::ColumnInfo & col_info);

DecodedFields::const_iterator findByColumnID(const Int64 col_id, const DecodedFields & row);

/// TiDB row encode/decode, handles both row format V1/V2, and respects schema (outer should take care of schema mismatching cases).
DecodedRow * decodeRow(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut);

/// Decode an unknown column's raw value (as string) in V2 row format, based on the given column info.
/// It is used for force decoding when the column info is certain, before which pre-decoding hasn't seen its column info yet thus can only record its raw value.
Field decodeUnknownColumnV2(const Field & unknown, const ColumnInfo & column_info);

/// The following two encode functions are used for testing.
void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, std::stringstream & ss);
void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, std::stringstream & ss);

} // namespace DB
