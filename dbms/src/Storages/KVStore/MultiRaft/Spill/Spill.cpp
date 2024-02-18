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
        txns[tso] = std::make_shared<SpillingTxn>();
    }
}

void Region::checkAndCommitBigTxn(const Timestamp &) {
    // TODO
    // Check if the lock cf exists. And ingest all SpillFile into DM.
}

void Region::meetLargeTxnLock(const Timestamp & tso) {

}

std::vector<TiKVKey> KVStore::findSpillableTxn(const RegionPtr & region) {
    // TODO
    return {};
}

bool KVStore::canSpillRegion(const RegionPtr & region, RegionTaskLock &) const {
    // TODO
    // Is split contradict with persist?
    return false;
}

SpilledMemtable KVStore::maybeSpillDefaultCf(RegionPtr & region, RegionTaskLock & l) {
    SpilledMemtable spilled_memtable;
    auto spillable_txns = findSpillableTxn(region);
    if (!spillable_txns.empty() && canSpillRegion(region, l)) {
        for (const auto & start_ts : spillable_txns) {
            spilled_memtable += region->maybeSpillDefaultCf(start_ts);
        }
    }
    return spilled_memtable;
}

} // namespace DB