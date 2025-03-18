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

#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <Storages/KVStore/Region_fwd.h>
#include <TiDB/Schema/TiDB_fwd.h>

namespace DB
{

class MockTiKV : public ext::Singleton<MockTiKV>
{
    friend class ext::Singleton<MockTiKV>;

public:
    // Generate a RegionPtr with given params.
    // The raft-index is set according to the mock raft-index on `MockTiKV` instance.
    RegionPtr createRegion( //
        TableID table_id,
        RegionID region_id,
        const HandleID & start,
        const HandleID & end);

    // Generate a RegionPtr with given params of common handle.
    // The raft-index is set according to the mock raft-index on `MockTiKV` instance.
    RegionPtr createRegionCommonHandle(
        const TiDB::TableInfo & table_info,
        RegionID region_id,
        std::vector<Field> & start_keys,
        std::vector<Field> & end_keys);

    // Generate multiple RegionPtrs with given params.
    // Each Region's raft-index is set according to the mock raft-index on `MockTiKV` instance
    Regions createRegions(
        TableID table_id,
        size_t region_num,
        size_t key_num_each_region,
        HandleID handle_begin,
        RegionID new_region_id_begin);

    UInt64 getNextRaftIndex(RegionID region_id)
    {
        std::lock_guard lock(mutex);
        auto it = raft_index.find(region_id);
        if (it == raft_index.end())
        {
            // Usually index 6 is empty and we ignore it.
            // https://github.com/tikv/tikv/issues/7047
            auto init_index = RAFT_INIT_LOG_INDEX + 1;
            it = raft_index.emplace_hint(it, region_id, init_index);
        }
        ++(it->second);
        return it->second;
    }

    UInt64 getRaftTerm(RegionID region_id)
    {
        std::lock_guard lock(mutex);
        auto it = raft_term.find(region_id);
        if (it == raft_term.end())
            it = raft_term.emplace_hint(it, region_id, RAFT_INIT_LOG_TERM);
        return it->second;
    }

private:
    std::mutex mutex;

    std::unordered_map<RegionID, UInt64> raft_index;
    std::unordered_map<RegionID, UInt64> raft_term;
};

} // namespace DB
