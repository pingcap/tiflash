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

#include <Core/QueryOperatorSpillContexts.h>

namespace DB
{
Int64 QueryOperatorSpillContexts::triggerAutoSpill(Int64 expected_released_memories, bool ignore_cooldown_time_check)
{
    std::unique_lock lock(mutex, std::try_to_lock);
    /// use mutex to avoid concurrent check
    if (lock.owns_lock())
    {
        auto log_level = Poco::Message::PRIO_DEBUG;
        bool check_cooldown_time = !ignore_cooldown_time_check;
        if unlikely (!first_check_done)
        {
            first_check_done = true;
            check_cooldown_time = false;
            log_level = Poco::Message::PRIO_INFORMATION;
        }

        auto current_time = watch.elapsed();
        if (check_cooldown_time && current_time - last_checked_time_ns < auto_spill_check_min_interval_ns)
        {
            return expected_released_memories;
        }

        LOG_IMPL(
            log,
            log_level,
            "Query memory usage exceeded threshold, trigger auto spill check, expected released memory: {}",
            expected_released_memories);

        last_checked_time_ns = current_time;

        auto ret = expected_released_memories;

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
            ret = pair.second->triggerAutoSpill(ret);
            if (ret <= 0)
                break;
        }
        LOG_IMPL(
            log,
            log_level,
            "Auto spill check finished, marked {} memory to be spilled",
            expected_released_memories - ret);
        return ret;
    }
    return expected_released_memories;
}
} // namespace DB
