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

#include <Storages/KVStore/MultiRaft/Disagg/IncrementalSnapshot.h>
#include <Storages/KVStore/Region.h>

#include <set>

namespace DB
{
void IncrementalSnapshotMgr::observeDeltaSummary(
    UInt64 persisted_applied_index,
    RegionPtr region,
    UInt64 * l0_ids,
    UInt64 l0_ids_size)
{
    if (!l0_ids_size)
        return;
    summary = IncrementalSnapshotProto::DeltaSummary();
    for (UInt64 i = 0; i < l0_ids_size; i++)
    {
        summary.add_l0_ids(l0_ids[i]);
    }
    summary.set_persisted_applied_index(persisted_applied_index);
    summary.set_ver(region->version());
    summary.set_start(region->getRange()->rawKeys().first->toString());
    summary.set_end(region->getRange()->rawKeys().second->toString());
}

std::optional<ReuseSummary> IncrementalSnapshotMgr::tryReuseDeltaSummary(
    UInt64 applied_index,
    RegionPtr region,
    UInt64 * new_l0_ids,
    UInt64 new_l0_id_size)
{
    // Find a summary with `persisted_applied_index` less than `applied_index`.
    // If there are more than one options(after later optimazation), choose the largest one.
    if (applied_index <= summary.persisted_applied_index())
    {
        return std::nullopt;
    }

    if (summary.ver() != region->version())
    {
        // TODO(is) support split
        return std::nullopt;
    }

    auto prev_set = std::unordered_set<UInt64>(summary.l0_ids().cbegin(), summary.l0_ids().cend());
    auto new_set = std::unordered_set<UInt64>(new_l0_ids, new_l0_ids + new_l0_id_size);

    ReuseSummary reuse;
    for (auto e : prev_set)
    {
        if (!new_set.count(e))
        {
            // `e` is compacted.
            return std::nullopt;
        }
    }
    for (auto e : new_set)
    {
        if (prev_set.count(e))
        {
            reuse.ids.push_back(e);
        }
    }

    return reuse;
}

String IncrementalSnapshotMgr::serializeToString() const
{
    IncrementalSnapshotProto::DeltaSummariesPersisted persisted;
    *persisted.mutable_summary() = summary;
    return persisted.SerializeAsString();
}

void IncrementalSnapshotMgr::deserializeFromString(const String & s)
{
    IncrementalSnapshotProto::DeltaSummariesPersisted persisted;
    if (!persisted.ParseFromArray(s.data(), s.size()))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't parse DeltaSummariesPersisted");
    }
    summary = persisted.summary();
}

} // namespace DB