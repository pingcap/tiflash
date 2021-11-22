#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/TiDB.h>
#include <Core/TMTPKType.h>


namespace DB
{
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
struct DecodingStorageSchemaSnapshot
{
    DM::ColumnDefinesPtr column_defines;
    // column id -> column pos in column_defines
    SortedColumnIDWithPos sorted_column_id_with_pos;

    // 1. when the table doesn't have a common handle,
    //    1) if `pk_is_handle` is false, `pk_column_ids` is empty
    //    2) if `pk_is_handle` is true, `pk_column_ids` contain a single element which is the column id of the pk column
    // 2. when the table has a common handle, `pk_column_ids` contain the column ids of all pk columns,
    //    and the order in `pk_column_ids` is the order to decode pk columns from the common handle
    std::vector<ColumnID> pk_column_ids;
    // the pos in the column list which is sorted by column id
    std::map<ColumnID, size_t> pk_pos_map;

    bool is_common_handle;
    TMTPKType pk_type = TMTPKType::UNSPECIFIED;
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;

    DecodingStorageSchemaSnapshot(DM::ColumnDefinesPtr column_defines_, const TiDB::TableInfo & table_info_, const DM::ColumnDefine & original_handle_, bool is_common_handle_)
        : column_defines{std::move(column_defines_)}, is_common_handle{is_common_handle_}, schema_version{table_info_.schema_version}
    {
        for (size_t i = 0; i < column_defines->size(); i++)
        {
            auto & column_define = (*column_defines)[i];
            sorted_column_id_with_pos.insert({column_define.id, i});
        }

        // create pk related metadata if needed
        if (is_common_handle)
        {
            auto & primary_index_info = table_info_.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
                pk_column_ids.emplace_back(table_info_.columns[primary_index_info.idx_cols[i].offset].id);
                pk_pos_map.emplace(original_handle_.id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max));
            }
            pk_type = TMTPKType::STRING;
        }
        else if (table_info_.pk_is_handle)
        {
            pk_column_ids.emplace_back(original_handle_.id);
            pk_pos_map.emplace(original_handle_.id, reinterpret_cast<size_t>(std::numeric_limits<size_t>::max));
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

}
