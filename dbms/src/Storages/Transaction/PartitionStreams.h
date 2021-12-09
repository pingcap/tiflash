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
 * A snapshot of the table structure of a DeltaTree storage. We use it to decode Raft snapshot
 * data with a consistent table structure.
 * TODO: consider refactoring the table structure related classes
 * Now there are some classes in IStorage/IManageableStorage/DeltaMergeStore level are both
 * related to the table structure. It make applying DDL operations and decoding Raft data
 * more complicated.
 */
struct DecodingStorageSchemaSnapshot
{
    bool is_common_handle = false;
    TiDB::TableInfo table_info;
    ColumnsDescription columns;
    DM::ColumnDefinesPtr column_defines;
    DM::ColumnDefine original_table_handle_define;


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
