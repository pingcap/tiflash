#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
class StorageDeltaMerge;

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
    // columns in `block` are also ordered by column id
    Block block;
    // 1. when `pk_is_handle` is true, `pk_column_ids` contain a single element which is the column id of the pk column
    // 2. when the table has a common handle, `pk_column_ids` contain the column ids of all pk columns,
    //    and the order in `pk_column_ids` is the order to decode pk columns from the common handle
    std::vector<ColumnID> pk_column_ids;
    std::map<ColumnID, size_t> pk_pos_map;

    DecodingStorageSchemaSnapshot() = default;

    DecodingStorageSchemaSnapshot(const DecodingStorageSchemaSnapshot &) = delete;
    DecodingStorageSchemaSnapshot & operator=(const DecodingStorageSchemaSnapshot &) = delete;

    DecodingStorageSchemaSnapshot(DecodingStorageSchemaSnapshot &&) = default;
    DecodingStorageSchemaSnapshot & operator=(DecodingStorageSchemaSnapshot &&) = default;
};

std::tuple<TableLockHolder, std::shared_ptr<StorageDeltaMerge>, DecodingStorageSchemaSnapshot> //
AtomicGetStorageSchema(const RegionPtr & region, TMTContext & tmt);

Block GenRegionBlockDataWithSchema(const RegionPtr & region, //
    const DecodingStorageSchemaSnapshot & schema_snap,
    Timestamp gc_safepoint,
    bool force_decode,
    TMTContext & tmt);

} // namespace DB
