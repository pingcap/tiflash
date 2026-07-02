// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/Context_fwd.h>
#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/KVStore/Decode/RegionTable_fwd.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <mutex>

namespace DB
{

using SafeTS = UInt64;
enum : SafeTS
{
    InvalidSafeTS = std::numeric_limits<UInt64>::max(),
};

using TsoShiftBits = UInt64;
enum : TsoShiftBits
{
    TsoPhysicalShiftBits = 18,
};

struct SafeTsManager
{
    // safe ts is maintained by check_leader RPC (https://github.com/tikv/tikv/blob/1ea26a2ac8761af356cc5c0825eb89a0b8fc9749/components/resolved_ts/src/advance.rs#L262),
    // leader_safe_ts is the safe_ts in leader, leader will send <applied_index, safe_ts> to learner to advance safe_ts of learner, and TiFlash will record the safe_ts into safe_ts_map in check_leader RPC.
    // self_safe_ts is the safe_ts in TiFlash learner. When TiFlash proxy receive <applied_index, safe_ts> from leader, TiFlash will update safe_ts_map when TiFlash has applied the raft log to applied_index.
    struct SafeTsEntry
    {
        explicit SafeTsEntry(SafeTS leader_safe_ts, SafeTS self_safe_ts)
            : leader_safe_ts(leader_safe_ts)
            , self_safe_ts(self_safe_ts)
        {}
        std::atomic<SafeTS> leader_safe_ts;
        std::atomic<SafeTS> self_safe_ts;
    };
    using SafeTsEntryPtr = std::unique_ptr<SafeTsEntry>;
    using SafeTsMap = std::unordered_map<RegionID, SafeTsEntryPtr>;

    void updateSafeTS(RegionID region_id, SafeTS leader_safe_ts, SafeTS self_safe_ts);

    // unit: ms. If safe_ts diff is larger than 2min, we think the data synchronization progress is far behind the leader.
    static const SafeTS SafeTsDiffThreshold = 2 * 60 * 1000;
    bool isSafeTSLag(RegionID region_id, SafeTS * leader_safe_ts, SafeTS * self_safe_ts);

    struct SafeTsPair
    {
        const SafeTS leader_safe_ts;
        const SafeTS self_safe_ts;
    };
    SafeTsPair get(RegionID region_id) const
    {
        std::shared_lock read_lock(rw_lock);
        if (auto iter = safe_ts_map.find(region_id); iter != safe_ts_map.end())
        {
            return SafeTsPair{
                .leader_safe_ts = (*iter->second).leader_safe_ts.load(),
                .self_safe_ts = (*iter->second).self_safe_ts.load(),
            };
        }
        return SafeTsPair{0, 0};
    }

    UInt64 getSelfSafeTS(RegionID region_id) const;

    void remove(RegionID region_id)
    {
        std::unique_lock write_lock(rw_lock);
        safe_ts_map.erase(region_id);
    }
    void clear()
    {
        std::unique_lock write_lock(rw_lock);
        safe_ts_map.clear();
    }

private:
    SafeTsMap safe_ts_map;
    mutable std::shared_mutex rw_lock;
};
} // namespace DB
