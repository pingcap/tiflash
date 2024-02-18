// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/MultiRaft/Spill/Spill.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>

namespace DB {

void SpillTxnCtx::meetLargeTxnLock(const Timestamp & tso) {
    if (!txns.contains(tso)) {
        txns.emplace(tso, std::make_shared<LargeTxn>());
    }
}

std::optional<std::pair<Timestamp, LargeTxnPtr>>> SpillTxnCtx::pickOne() const {
    if likely(txns.empty()) return std::nullopt;
    // TODO(spill) pick the largest one.
    return *txns.begin();
}

void Region::checkAndCommitLargeTxn(const Timestamp &) {
    // TODO(spill)
    // Check if the lock cf exists. And ingest all SpillFile into DM.
}

bool KVStore::canSpillRegion(const RegionPtr &, RegionTaskLock &) const {
    // TODO(spill) Is split contradict with persist?
    return false;
}

std::optional<SpilledMemtable> KVStore::maybeSpillDefaultCf(RegionPtr & region, RegionTaskLock &) {
    auto txn = region->getSpillTxnCtx().pickOne();
    if likely(!txn.has_value()) return std::nullopt;
    if (!canSpillRegion(region, l)) return std::nullopt;
    SpilledMemtable spilled_memtable;
    region->spillMemtable(spilled_memtable);
    return spilled_memtable;
}

void Region::spillMemtable(SpilledMemtable & spilled_memtable, RegionTaskLock &) {
    // In this case, most of the key-value pairs blong to large txn,
    // So we move backward.
    spilled_memtable.default_cf = data.takeDefaultCf();
    // TODO(spill) improve performance
    
}

} // namespace DB