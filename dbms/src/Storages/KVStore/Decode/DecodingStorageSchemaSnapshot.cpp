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

#include <DataTypes/DataTypeString.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
DecodingStorageSchemaSnapshot::DecodingStorageSchemaSnapshot(
    DM::ColumnDefinesPtr column_defines_,
    const TiDB::TableInfo & table_info_,
    const DM::ColumnDefine & original_handle_,
    Int64 decoding_schema_epoch_,
    bool with_version_column)
    : column_defines{std::move(column_defines_)}
    , pk_is_handle{table_info_.pk_is_handle}
    , is_common_handle{table_info_.is_common_handle}
    , decoding_schema_epoch{decoding_schema_epoch_}
{
    std::unordered_map<ColumnID, size_t> column_lut(table_info_.columns.size());
    // col id -> tidb pos, has no internal cols.
    for (size_t i = 0; i < table_info_.columns.size(); i++)
    {
        const auto & ci = table_info_.columns[i];
        column_lut.emplace(ci.id, i);
    }
    // column_defines has internal cols.
    size_t index_in_block = 0;
    for (size_t i = 0; i < column_defines->size(); i++)
    {
        auto & cd = (*column_defines)[i];
        if (cd.id != VersionColumnID || with_version_column)
        {
            col_id_to_block_pos.insert({cd.id, index_in_block++});
        }
        col_id_to_def_pos.insert({cd.id, i});

        if (cd.id != TiDBPkColumnID && cd.id != VersionColumnID && cd.id != DelMarkColumnID)
        {
            const auto & columns = table_info_.columns;
            column_infos.push_back(columns[column_lut.at(cd.id)]);
        }
        else
        {
            column_infos.push_back(TiDB::ColumnInfo());
        }
    }

    // create pk related metadata if needed
    if (is_common_handle)
    {
        const auto & primary_index_cols = table_info_.getPrimaryIndexInfo().idx_cols;
        for (const auto & primary_index_col : primary_index_cols)
        {
            auto pk_column_id = table_info_.columns[primary_index_col.offset].id;
            pk_column_ids.emplace_back(pk_column_id);
            pk_pos_map.emplace(pk_column_id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max()));
        }
        pk_type = TMTPKType::STRING;
        rowkey_column_size = pk_column_ids.size();
    }
    else if (table_info_.pk_is_handle)
    {
        pk_column_ids.emplace_back(original_handle_.id);
        pk_pos_map.emplace(original_handle_.id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max()));
        pk_type = getTMTPKType(*original_handle_.type);
        rowkey_column_size = 1;
    }
    else
    {
        pk_type = TMTPKType::INT64;
        rowkey_column_size = 1;
    }

    // calculate pk column pos in block
    if (!pk_pos_map.empty())
    {
        auto pk_pos_iter = pk_pos_map.begin();
        size_t column_pos_in_block = 0;
        for (auto & column_id_with_pos : col_id_to_block_pos)
        {
            if (pk_pos_iter == pk_pos_map.end())
                break;
            if (pk_pos_iter->first == column_id_with_pos.first)
            {
                pk_pos_iter->second = column_pos_in_block;
                pk_pos_iter++;
            }
            column_pos_in_block++;
        }
        if (unlikely(pk_pos_iter != pk_pos_map.end()))
            throw Exception("Cannot find all pk columns in block", ErrorCodes::LOGICAL_ERROR);
    }
}


TMTPKType getTMTPKType(const IDataType & rhs)
{
    static const DataTypeInt64 & dataTypeInt64 = {}; // NOLINT
    static const DataTypeUInt64 & dataTypeUInt64 = {}; // NOLINT
    static const DataTypeString & dataTypeString = {}; // NOLINT

    if (rhs.equals(dataTypeInt64))
        return TMTPKType::INT64;
    else if (rhs.equals(dataTypeUInt64))
        return TMTPKType::UINT64;
    else if (rhs.equals(dataTypeString))
        return TMTPKType::STRING;
    return TMTPKType::UNSPECIFIED;
}

Block createBlockSortByColumnID(DecodingStorageSchemaSnapshotConstPtr schema_snapshot, bool with_version_column)
{
    Block block;
    // # Safety
    // Though `col_id_to_block_pos` lacks some fields in `col_id_to_def_pos`,
    // it is always a sub-sequence of `col_id_to_def_pos`.
    for (const auto & [col_id, def_pos] : schema_snapshot->getColId2DefPosMap())
    {
        // col_id == cd.id
        // Including some internal columns:
        // - (VersionColumnID, _INTERNAL_VERSION, u64)
        // - (DelMarkColumnID, _INTERNAL_DELMARK, u8)
        // - (TiDBPkColumnID, _tidb_rowid, i64)
        auto & cd = (*(schema_snapshot->column_defines))[def_pos];
        if (!with_version_column && cd.id == VersionColumnID)
            continue;
        block.insert({cd.type->createColumn(), cd.type, cd.name, col_id});
    }
    return block;
}

void clearBlockData(Block & block)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        auto * raw_column = const_cast<IColumn *>((block.getByPosition(i)).column.get());
        raw_column->popBack(raw_column->size());
    }
}
} // namespace DB
