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

namespace DB {

void IncrementalSnapshotManager::observeDeltaSummary(UInt64 persisted_applied_index, RegionPtr region, UInt64 * l0_ids, UInt64 l0_ids_size) {
    if (!l0_ids_size) return;
    IncrementalSnapshotProto::DeltaSummary summary;
    for (UInt64 i = 0; i < l0_ids_size; i++) {
        summary.add_l0_ids(l0_ids[i]);
    }
    summary.set_persisted_applied_index(persisted_applied_index);
    summary.set_ver(region->version());
    summary.set_start(region->getRange()->rawKeys().first->toString());
    summary.set_end(region->getRange()->rawKeys().second->toString());
    summaries.insert(std::make_pair(persisted_applied_index, std::move(summary)));
}

std::optional<IncrementalSnapshotProto::DeltaSummary> IncrementalSnapshotManager::tryReuseDeltaSummary(UInt64 applied_index, RegionPtr region) {
    auto it = summaries.lower_bound(applied_index);
    if(it != summaries.begin()) {
        it --;
    } else {
        return std::nullopt;
    }
    // Find a summary with `persisted_applied_index` less than `applied_index`.
    // If there are more than one options, choose the largest one.

    if(it->second.ver() != region->version())
        // TODO(is) support split
        return std::nullopt;
    
    return it->second;
}

bool IncrementalSnapshotManager::truncateByPersistAdvance(UInt64 persisted_applied_index) {
    // Remove all elements before `remove_it`.
    bool removed = false;
    auto remove_it = summaries.begin();
    for(; remove_it != summaries.end() ; ) {
        if (remove_it->first < persisted_applied_index) {
            removed = true;
            remove_it = summaries.erase(remove_it);
        }
        else
            break;
    }
    return removed;
}

String IncrementalSnapshotManager::serializeToString() const {
    IncrementalSnapshotProto::DeltaSummariesPersisted persisted_summaries;
    for (auto it = summaries.cbegin(); it != summaries.cend(); it++) {
        *persisted_summaries.add_summaries() = it->second;
    }
    return persisted_summaries.SerializeAsString();
}

void IncrementalSnapshotManager::deserializeFromString(const String & s) {
    IncrementalSnapshotProto::DeltaSummariesPersisted persisted_summaries;
    if (!persisted_summaries.ParseFromArray(s.data(), s.size()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't parse DeltaSummariesPersisted");
    }
    for (auto summary : persisted_summaries.summaries()) {
        summaries.insert(std::make_pair(summary.persisted_applied_index(), summary));
    }
}

} // namespace DB