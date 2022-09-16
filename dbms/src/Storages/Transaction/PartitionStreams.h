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
