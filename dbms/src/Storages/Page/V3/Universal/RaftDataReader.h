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

#include <Storages/Page/V3/Universal/UniversalPageStorage.h>

namespace DB
{
class RaftDataReader final
{
public:
    explicit RaftDataReader(UniversalPageStorage & storage)
        : uni_ps(storage)
    {}

    Page read(const UniversalPageId & page_id);

    // scan all pages in range [start, end)
    // if end is empty, it will be transformed to a key larger than all raft data key
    void traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor);

    // Only used to get raft log data from remote checkpoint data
    void traverseRemoteRaftLogForRegion(UInt64 region_id, const std::function<void(const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation & location)> & acceptor);

    // return the first id not less than `page_id`
    std::optional<UniversalPageId> getLowerBound(const UniversalPageId & page_id);

private:
    static char raft_data_end_key[1];

private:
    UniversalPageStorage & uni_ps;
};
} // namespace DB
