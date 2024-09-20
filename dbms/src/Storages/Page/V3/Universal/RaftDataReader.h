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

#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
namespace DB
{
class RaftDataReader final
{
public:
    explicit RaftDataReader(UniversalPageStorage & storage)
        : uni_ps(storage)
    {}

    Page read(const UniversalPageId & page_id) const;

    std::optional<raft_serverpb::RaftApplyState> readRegionApplyState(RegionID region_id) const;

    // scan all pages in range [start, end)
    // if end is empty, it will be transformed to a key larger than all raft data key
    void traverse(
        const UniversalPageId & start,
        const UniversalPageId & end,
        const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor);

    // Only used to get raft log data from remote checkpoint data
    void traverseRemoteRaftLogForRegion(
        UInt64 region_id,
        const std::function<bool(size_t)> precheck,
        const std::function<
            void(const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation & location)> &
            acceptor);

    // return the first id not less than `page_id`
    std::optional<UniversalPageId> getLowerBound(const UniversalPageId & page_id);

    // return region id if the `page_id` is a key for a raft related data
    static std::optional<UInt64> tryParseRegionId(const UniversalPageId & page_id);

    // return raft log index if the `page_id` is a key for a raft log
    static std::optional<UInt64> tryParseRaftLogIndex(const UniversalPageId & page_id);

private:
    static char raft_data_end_key[1];

private:
    UniversalPageStorage & uni_ps;
};
} // namespace DB
