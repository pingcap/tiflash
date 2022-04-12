// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/TiDB.h>


namespace DB
{
enum TMTPKType
{
    INT64,
    UINT64,
    STRING,
    UNSPECIFIED,
};

TMTPKType getTMTPKType(const IDataType & rhs);

/**
 * A snapshot of the table structure of a storage(just support DeltaTree storage now). We use it to decode Raft snapshot
 * data with a consistent table structure.
 * TODO: consider refactoring the table structure related classes
 * Now there are some classes in IStorage/IManageableStorage/DeltaMergeStore level are both
 * related to the table structure. It make applying DDL operations and decoding Raft data
 * more complicated.
 */
using SortedColumnIDWithPos = std::map<ColumnID, size_t>;
using SortedColumnIDWithPosConstIter = SortedColumnIDWithPos::const_iterator;
using TableInfo = TiDB::TableInfo;
using ColumnInfo = TiDB::ColumnInfo;
using ColumnInfos = std::vector<ColumnInfo>;
struct DecodingStorageSchemaSnapshot
{
    // There is a one-to-one correspondence between elements in `column_defines` and elements in `column_infos`
    // Note that some columns(EXTRA_HANDLE_COLUMN, VERSION_COLUMN, TAG_COLUMN) may not be a real column in tidb schema,
    // so their corresponding elements in `column_infos` are just nullptr and won't be used when decoding.
    DM::ColumnDefinesPtr column_defines;
    ColumnInfos column_infos;

    // column id -> column pos in column_defines/column_infos
    SortedColumnIDWithPos sorted_column_id_with_pos;

    // 1. when the table doesn't have a common handle,
    //    1) if `pk_is_handle` is false, `pk_column_ids` is empty
    //    2) if `pk_is_handle` is true, `pk_column_ids` contain a single element which is the column id of the pk column
    // 2. when the table has a common handle, `pk_column_ids` contain the column ids of all pk columns,
    //    and the order in `pk_column_ids` is the order to decode pk columns from the common handle
    std::vector<ColumnID> pk_column_ids;
    // the pos in the column list which is sorted by column id
    std::map<ColumnID, size_t> pk_pos_map;

    bool pk_is_handle;
    bool is_common_handle;
    TMTPKType pk_type = TMTPKType::UNSPECIFIED;
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;

    DecodingStorageSchemaSnapshot(DM::ColumnDefinesPtr column_defines_, const TiDB::TableInfo & table_info_, const DM::ColumnDefine & original_handle_)
        : column_defines{std::move(column_defines_)}
        , pk_is_handle{table_info_.pk_is_handle}
        , is_common_handle{table_info_.is_common_handle}
        , schema_version{table_info_.schema_version}
    {
        std::unordered_map<ColumnID, size_t> column_lut;
        for (size_t i = 0; i < table_info_.columns.size(); i++)
        {
            const auto & ci = table_info_.columns[i];
            column_lut.emplace(ci.id, i);
        }
        for (size_t i = 0; i < column_defines->size(); i++)
        {
            auto & cd = (*column_defines)[i];
            sorted_column_id_with_pos.insert({cd.id, i});
            if (cd.id != TiDBPkColumnID && cd.id != VersionColumnID && cd.id != DelMarkColumnID)
            {
                auto & columns = table_info_.columns;
                column_infos.push_back(columns[column_lut.at(cd.id)]);
            }
            else
            {
                column_infos.push_back(ColumnInfo());
            }
        }

        // create pk related metadata if needed
        if (is_common_handle)
        {
            const auto & primary_index_info = table_info_.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
                auto pk_column_id = table_info_.columns[primary_index_info.idx_cols[i].offset].id;
                pk_column_ids.emplace_back(pk_column_id);
                pk_pos_map.emplace(pk_column_id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max()));
            }
            pk_type = TMTPKType::STRING;
        }
        else if (table_info_.pk_is_handle)
        {
            pk_column_ids.emplace_back(original_handle_.id);
            pk_pos_map.emplace(original_handle_.id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max()));
            pk_type = getTMTPKType(*original_handle_.type);
        }
        else
        {
            pk_type = TMTPKType::INT64;
        }

        // calculate pk column pos in block
        if (!pk_pos_map.empty())
        {
            auto pk_pos_iter = pk_pos_map.begin();
            size_t column_pos_in_block = 0;
            for (auto iter = sorted_column_id_with_pos.begin(); iter != sorted_column_id_with_pos.end(); iter++)
            {
                if (pk_pos_iter == pk_pos_map.end())
                    break;
                if (pk_pos_iter->first == iter->first)
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

    DecodingStorageSchemaSnapshot(const DecodingStorageSchemaSnapshot &) = delete;
    DecodingStorageSchemaSnapshot & operator=(const DecodingStorageSchemaSnapshot &) = delete;

    DecodingStorageSchemaSnapshot(DecodingStorageSchemaSnapshot &&) = default;
};
using DecodingStorageSchemaSnapshotPtr = std::shared_ptr<DecodingStorageSchemaSnapshot>;
using DecodingStorageSchemaSnapshotConstPtr = std::shared_ptr<const DecodingStorageSchemaSnapshot>;

Block createBlockSortByColumnID(DecodingStorageSchemaSnapshotConstPtr schema_snapshot);

void clearBlockData(Block & block);

} // namespace DB
