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

#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>


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
    DecodingStorageSchemaSnapshot(
        DM::ColumnDefinesPtr column_defines_,
        const TiDB::TableInfo & table_info_,
        const DM::ColumnDefine & original_handle_,
        Int64 decoding_schema_epoch_,
        bool with_version_column);

    DISALLOW_COPY(DecodingStorageSchemaSnapshot);

    DecodingStorageSchemaSnapshot(DecodingStorageSchemaSnapshot &&) = default;

    const SortedColumnIDWithPos & getColId2BlockPosMap() const { return col_id_to_block_pos; }
    const SortedColumnIDWithPos & getColId2DefPosMap() const { return col_id_to_def_pos; }

    // There is a one-to-one correspondence between elements in `column_defines` and elements in `column_infos`
    // Note that some columns(EXTRA_HANDLE_COLUMN, VERSION_COLUMN, TAG_COLUMN) may not be a real column in tidb schema,
    // so their corresponding elements in `column_infos` are just nullptr and won't be used when decoding.
    DM::ColumnDefinesPtr column_defines;
    ColumnInfos column_infos;

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
    size_t rowkey_column_size;
    TMTPKType pk_type = TMTPKType::UNSPECIFIED;
    // an internal increasing version for `DecodingStorageSchemaSnapshot`, has no relation with the table schema version
    Int64 decoding_schema_epoch;

private:
    // `col_id_to_def_pos` is originally `sorted_column_id_with_pos`.
    // We may omit some cols in block, e.g. version col. So `col_id_to_def_pos` may have more items than `col_id_to_block_pos`.
    // Both of the maps are sorted in ColumnID order, which makes the internal cols in first.
    SortedColumnIDWithPos col_id_to_block_pos;
    SortedColumnIDWithPos col_id_to_def_pos;
};
using DecodingStorageSchemaSnapshotPtr = std::shared_ptr<DecodingStorageSchemaSnapshot>;
using DecodingStorageSchemaSnapshotConstPtr = std::shared_ptr<const DecodingStorageSchemaSnapshot>;

Block createBlockSortByColumnID(DecodingStorageSchemaSnapshotConstPtr schema_snapshot, bool with_version_column = true);

void clearBlockData(Block & block);

} // namespace DB
