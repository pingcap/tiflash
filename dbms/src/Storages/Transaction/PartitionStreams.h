#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{
class Region;
using RegionPtr = std::shared_ptr<Region>;
class StorageDeltaMerge;
class TMTContext;

std::optional<RegionDataReadInfoList> ReadRegionCommitCache(const RegionPtr & region, bool lock_region);

std::tuple<TableLockHolder, std::shared_ptr<StorageDeltaMerge>, DecodingStorageSchemaSnapshotConstPtr> //
AtomicGetStorageSchema(const RegionPtr & region, TMTContext & tmt);

Block GenRegionBlockDataWithSchema(const RegionPtr & region, //
                                   const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
                                   Timestamp gc_safepoint,
                                   bool force_decode,
                                   TMTContext & tmt);

} // namespace DB
