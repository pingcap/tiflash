#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
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
using SortedColumnIDs = std::set<ColumnID>;
struct DecodingStorageSchemaSnapshot
{
    DM::ColumnDefinesPtr column_defines;

    SortedColumnIDs sorted_column_ids;
    // 1. when the table doesn't have a common handle,
    //    1) if `pk_is_handle` is false, `pk_column_ids` is empty
    //    2) if `pk_is_handle` is true, `pk_column_ids` contain a single element which is the column id of the pk column
    // 2. when the table has a common handle, `pk_column_ids` contain the column ids of all pk columns,
    //    and the order in `pk_column_ids` is the order to decode pk columns from the common handle
    std::vector<ColumnID> pk_column_ids;
    std::map<ColumnID, size_t> pk_pos_map;

    const TiDB::TableInfo & table_info;
    std::map<ColumnID, size_t> column_pos_in_table_info;

    bool is_common_handle;
    TMTPKType pk_type = TMTPKType::UNSPECIFIED;

    // help to create columns for block in sort column id
    std::map<ColumnID, size_t> column_pos_in_cd;

    DecodingStorageSchemaSnapshot(const TiDB::TableInfo & table_info_) : table_info{table_info_} {}

    DecodingStorageSchemaSnapshot(const DecodingStorageSchemaSnapshot &) = delete;
    DecodingStorageSchemaSnapshot & operator=(const DecodingStorageSchemaSnapshot &) = delete;

    DecodingStorageSchemaSnapshot(DecodingStorageSchemaSnapshot &&) = default;
};
using DecodingStorageSchemaSnapshotPtr = std::shared_ptr<DecodingStorageSchemaSnapshot>;
using DecodingStorageSchemaSnapshotConstPtr = std::shared_ptr<const DecodingStorageSchemaSnapshot>;

}
