#pragma once

#include <Core/Block.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{
using TiDB::ColumnInfo;
using TiDB::TableInfo;

/// The following two encode functions are used for testing.
void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);
void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);

bool appendRowToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id, // when pk is handle, we need skip pk column when decoding value
    bool force_decode);

bool appendRowV2ToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool force_decode);

template <bool is_big>
bool appendRowV2ToBlockImpl(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool force_decode);

bool appendRowV1ToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool force_decode);

} // namespace DB
