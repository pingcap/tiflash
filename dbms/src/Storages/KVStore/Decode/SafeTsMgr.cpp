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

#include <Storages/KVStore/Decode/SafeTsMgr.h>

namespace DB
{
bool SafeTsMgr::isSafeTSLag(UInt64 region_id, UInt64 * leader_safe_ts, UInt64 * self_safe_ts)
{
    {
        std::shared_lock lock(rw_lock);
        auto it = safe_ts_map.find(region_id);
        if (it == safe_ts_map.end())
        {
            return false;
        }
        *leader_safe_ts = it->second->leader_safe_ts.load(std::memory_order_relaxed);
        *self_safe_ts = it->second->self_safe_ts.load(std::memory_order_relaxed);
    }
    LOG_TRACE(
        DB::Logger::get(),
        "region_id={} leader_safe_ts={} self_safe_ts={}",
        region_id,
        *leader_safe_ts,
        *self_safe_ts);
    return (*leader_safe_ts > *self_safe_ts)
        && ((*leader_safe_ts >> TsoPhysicalShiftBits) - (*self_safe_ts >> TsoPhysicalShiftBits) > SafeTsDiffThreshold);
}

UInt64 SafeTsMgr::getSelfSafeTS(UInt64 region_id) const
{
    std::shared_lock lock(rw_lock);
    auto it = safe_ts_map.find(region_id);
    if (it == safe_ts_map.end())
    {
        return 0;
    }
    return it->second->self_safe_ts.load(std::memory_order_relaxed);
}

void SafeTsMgr::updateSafeTS(UInt64 region_id, UInt64 leader_safe_ts, UInt64 self_safe_ts)
{
    {
        std::shared_lock lock(rw_lock);
        auto it = safe_ts_map.find(region_id);
        if (it == safe_ts_map.end() && (leader_safe_ts == InvalidSafeTS || self_safe_ts == InvalidSafeTS))
        {
            LOG_TRACE(
                DB::Logger::get(),
                "safe_ts_map empty but safe ts invalid, region_id={} leader_safe_ts={} self_safe_ts={}",
                region_id,
                leader_safe_ts,
                self_safe_ts);
            return;
        }
        if (it != safe_ts_map.end())
        {
            if (leader_safe_ts != InvalidSafeTS)
            {
                it->second->leader_safe_ts.store(leader_safe_ts, std::memory_order_relaxed);
            }
            if (self_safe_ts != InvalidSafeTS)
            {
                it->second->self_safe_ts.store(self_safe_ts, std::memory_order_relaxed);
            }
            return;
        }
    }
    std::unique_lock lock(rw_lock);
    safe_ts_map.emplace(region_id, std::make_unique<SafeTsEntry>(leader_safe_ts, self_safe_ts));
}
} // namespace DB
