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

#include <Core/TaskOperatorSpillContexts.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
class QueryOperatorSpillContexts
{
public:
    explicit QueryOperatorSpillContexts(const MPPQueryId & query_id)
        : log(Logger::get(query_id.toString()))
    {}
    Int64 triggerAutoSpill(Int64 expected_released_memories)
    {
        std::unique_lock lock(mutex, std::try_to_lock);
        /// use mutex to avoid concurrent check, todo maybe need add minimum check interval(like 100ms) here?
        if (lock.owns_lock())
        {
            if unlikely (!first_check)
            {
                first_check = true;
                LOG_INFO(log, "Query memory usage exceeded threshold, trigger auto spill check");
            }
            /// vector of <revocable_memories, task_operator_spill_contexts>
            std::vector<std::pair<Int64, TaskOperatorSpillContexts *>> revocable_memories;
            revocable_memories.reserve(task_operator_spill_contexts_list.size());
            for (auto it = task_operator_spill_contexts_list.begin(); it != task_operator_spill_contexts_list.end();)
            {
                if ((*it)->isFinished())
                {
                    it = task_operator_spill_contexts_list.erase(it);
                }
                else
                {
                    revocable_memories.emplace_back((*it)->totalRevocableMemories(), (*it).get());
                    ++it;
                }
            }
            std::sort(revocable_memories.begin(), revocable_memories.end(), [](const auto & a, const auto & b) {
                return a.first > b.first;
            });
            for (auto & pair : revocable_memories)
            {
                if (pair.first < OperatorSpillContext::MIN_SPILL_THRESHOLD)
                    break;
                expected_released_memories = pair.second->triggerAutoSpill(expected_released_memories);
                if (expected_released_memories <= 0)
                    break;
            }
            return expected_released_memories;
        }
        return expected_released_memories;
    }

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
    bool first_check = false;
    LoggerPtr log;
    mutable std::mutex mutex;
};

} // namespace DB
