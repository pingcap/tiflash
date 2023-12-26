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

#include <Storages/KVStore/MultiRaft/Disagg/incremental_snapshot.pb.h>
#include <common/types.h>

#include <map>
#include <memory>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;

struct ReuseSummary
{
    std::vector<UInt64> ids;
};

struct IncrementalSnapshotManager
{
    void observeDeltaSummary(UInt64 persisted_applied_index, RegionPtr region, UInt64 * l0_ids, UInt64 l0_ids_size);
    std::optional<ReuseSummary> tryReuseDeltaSummary(
        UInt64 applied_index,
        RegionPtr region,
        UInt64 * new_l0_ids,
        UInt64 new_l0_id_size);
    String serializeToString() const;
    void deserializeFromString(const String &);

    IncrementalSnapshotProto::DeltaSummary summary;
    UInt64 region_id;
};

using IncrementalSnapshotManagerPtr = std::shared_ptr<IncrementalSnapshotManager>;
using UniqueIncrementalSnapshotManagerPtr = std::unique_ptr<IncrementalSnapshotManager>;

} // namespace DB