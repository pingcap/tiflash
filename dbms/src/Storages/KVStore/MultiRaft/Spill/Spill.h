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

#pragma once

#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/Types.h>
#include <unordered_map>

namespace DB {

// TODO(spill) track memory usage.
struct SpilledMemtable {
    RegionDefaultCFData default_cf;
    // Should we include RegionLockCFData? See following discussion.
};

using SpilledMemtablePtr = std::shared_ptr<SpilledMemtable>;
using SpilledMemtables = std::vector<SpilledMemtable>;
using SpilledMemtableMap = std::unordered_map<SpilledMemtable>;

struct LargeTxn {
    
};

using LargeTxnPtr = std::shared_ptr<LargeTxn>;
using LargeTxnMap = std::unordered_map<Timestamp, LargeTxnPtr>;

struct SpillTxnCtx {
    bool isLargeTxn(const Timestamp & ts) const {
        std::shared_lock l(mut);
        return txns.contains(ts);
    }
    bool hasLargeTxn() const {
        std::shared_lock l(mut);
        return !txns.empty();
    }
    void meetLargeTxnLock(const Timestamp & tso);
    std::optional<std::pair<Timestamp, LargeTxnPtr>> pickOne() const;
    // start_ts -> LargeTxn
    LargeTxnMap txns;
    mutable std::shared_mutex mut;
};

using SpillTxnCtxPtr = std::shared_ptr<LargeTxn>;

} // namespace DB