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
            /// vector of <index, revocable_memories>
            std::vector<std::pair<size_t, Int64>> revocable_memories(task_operator_spill_contexts_vec.size());
            bool has_finished_task = false;
            for (size_t i = 0; i < task_operator_spill_contexts_vec.size(); ++i)
            {
                revocable_memories[i] = std::make_pair(i, task_operator_spill_contexts_vec[i]->totalRevocableMemories());
                if (task_operator_spill_contexts_vec[i]->isFinished())
                    has_finished_task = true;
            }
            std::sort(revocable_memories.begin(), revocable_memories.end(), [](const std::pair<size_t, Int64> & a, std::pair<size_t, Int64> & b) {
                return a.second > b.second;
            });
            for (auto & pair : revocable_memories)
            {
                if (pair.second < OperatorSpillContext::MIN_SPILL_THRESHOLD)
                    break;
                expected_released_memories = task_operator_spill_contexts_vec[pair.first]->triggerAutoSpill(expected_released_memories);
                if (expected_released_memories <= 0)
                    break;
            }
            if (has_finished_task)
            {
                /// clean finished task
                task_operator_spill_contexts_vec.erase(std::remove_if(task_operator_spill_contexts_vec.begin(), task_operator_spill_contexts_vec.end(), [](const auto & contexts) { return contexts->isFinished(); }), task_operator_spill_contexts_vec.end());
            }
            return expected_released_memories;
        }
        return expected_released_memories;
    }

    void registerTaskOperatorSpillContexts(const std::shared_ptr<TaskOperatorSpillContexts> & task_operator_spill_contexts)
    {
        std::unique_lock lock(mutex);
        task_operator_spill_contexts_vec.push_back(task_operator_spill_contexts);
    }
    /// used for test
    size_t getTaskOperatorSpillContextsCount() const
    {
        return task_operator_spill_contexts_vec.size();
    }

private:
    std::vector<std::shared_ptr<TaskOperatorSpillContexts>> task_operator_spill_contexts_vec;
    bool first_check = false;
    LoggerPtr log;
    std::mutex mutex;
};

} // namespace DB
