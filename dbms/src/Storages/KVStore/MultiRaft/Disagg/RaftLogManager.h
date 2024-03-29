// Copyright 2023 PingCAP, Ltd.
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
#include <Storages/KVStore/Types.h>
#include <common/types.h>

#include <unordered_map>

namespace DB
{

struct RaftLogEagerGcHint
{
    UInt64 eager_truncate_index;
    UInt64 applied_index;
};
class RaftLogEagerGcTasks
{
public:
    // Check whether a new eager GC task for given region_id will be created.
    bool updateHint(RegionID region_id, UInt64 eager_truncated_index, UInt64 applied_index, UInt64 threshold);

    using Hints = std::map<RegionID, RaftLogEagerGcHint>;
    Hints getAndClearHints();

private:
    std::mutex mtx_tasks;
    Hints tasks;
};

// RegionID -> truncated index
using RaftLogGcTasksRes = std::unordered_map<RegionID, UInt64>;

[[nodiscard]] RaftLogGcTasksRes executeRaftLogGcTasks(Context & global_ctx, RaftLogEagerGcTasks::Hints && hints);

} // namespace DB
