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

#include <Common/Stopwatch.h>
#include <Core/TaskOperatorSpillContexts.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
class QueryOperatorSpillContexts
{
public:
    QueryOperatorSpillContexts(const MPPQueryId & query_id, UInt64 auto_spill_check_min_interval_ms)
        : auto_spill_check_min_interval_ns(auto_spill_check_min_interval_ms * 1000000ULL)
        , log(Logger::get(query_id.toString()))
    {
        watch.start();
    }

    Int64 triggerAutoSpill(Int64 expected_released_memories, bool ignore_cooldown_time_check = false);

    void registerTaskOperatorSpillContexts(
        const std::shared_ptr<TaskOperatorSpillContexts> & task_operator_spill_contexts)
    {
        std::unique_lock lock(mutex);
        task_operator_spill_contexts_list.push_back(task_operator_spill_contexts);
    }
    /// used for test
    size_t getTaskOperatorSpillContextsCount() const
    {
        std::unique_lock lock(mutex);
        return task_operator_spill_contexts_list.size();
    }

    const LoggerPtr & getLogger() const { return log; }

private:
    std::list<std::shared_ptr<TaskOperatorSpillContexts>> task_operator_spill_contexts_list;
    bool first_check_done = false;
    const UInt64 auto_spill_check_min_interval_ns;
    UInt64 last_checked_time_ns = 0;
    LoggerPtr log;
    mutable std::mutex mutex;
    Stopwatch watch;
};

} // namespace DB
