#pragma once

#include <Core/Block.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{
/// The following two encode functions are used for testing.
void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);
void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);

bool appendRowToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const DecodingStorageSchemaSnapshotConstPtr & schema_snapshot,
    bool force_decode);


} // namespace DB
