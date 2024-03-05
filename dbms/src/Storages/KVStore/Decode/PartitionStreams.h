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

#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/TableLockHolder.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
class Region;
using RegionPtr = std::shared_ptr<Region>;
class StorageDeltaMerge;
class TMTContext;
struct RegionPtrWithBlock;

std::optional<RegionDataReadInfoList> ReadRegionCommitCache(const RegionPtr & region, bool lock_region);
void RemoveRegionCommitCache(
    const RegionPtr & region,
    const RegionDataReadInfoList & data_list_read,
    bool lock_region = true);

std::tuple<TableLockHolder, std::shared_ptr<StorageDeltaMerge>, DecodingStorageSchemaSnapshotConstPtr> //
AtomicGetStorageSchema(const RegionPtr & region, TMTContext & tmt);

Block GenRegionBlockDataWithSchema(
    const RegionPtr & region, //
    const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
    Timestamp gc_safepoint,
    bool force_decode,
    TMTContext & tmt);

template <typename ReadList>
DM::WriteResult writeRegionDataToStorage(
    Context & context,
    const RegionPtrWithBlock & region,
    ReadList & data_list_read,
    const LoggerPtr & log);

} // namespace DB
