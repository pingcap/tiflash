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

#include <Core/Types.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/Types.h>

#include <unordered_map>
#include <vector>


namespace DB
{
struct RegionLearnerReadSnapshot : RegionPtr
{
    UInt64 snapshot_event_flag{0};

    RegionLearnerReadSnapshot() = default;
    explicit RegionLearnerReadSnapshot(const RegionPtr & region)
        : RegionPtr(region)
        , snapshot_event_flag(region->getSnapshotEventFlag())
    {}
    bool operator!=(const RegionPtr & rhs) const { return (rhs != *this) || (rhs && snapshot_event_flag != rhs->getSnapshotEventFlag()); }
};
using LearnerReadSnapshot = std::unordered_map<RegionID, RegionLearnerReadSnapshot>;

[[nodiscard]] LearnerReadSnapshot
doLearnerRead(
    const TiDB::TableID table_id,
    MvccQueryInfo & mvcc_query_info,
    size_t num_streams,
    bool for_batch_cop,
    Context & context,
    const LoggerPtr & log);

// After getting stream from storage, we must make sure regions' version haven't changed after learner read.
// If some regions' version changed, this function will throw `RegionException`.
void validateQueryInfo(
    const MvccQueryInfo & mvcc_query_info,
    const LearnerReadSnapshot & regions_snapshot,
    TMTContext & tmt,
    const LoggerPtr & log);

} // namespace DB
