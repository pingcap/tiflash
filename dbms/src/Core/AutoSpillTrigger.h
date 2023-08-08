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

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Core/QueryOperatorSpillContexts.h>

namespace DB
{
class AutoSpillTrigger
{
public:
    AutoSpillTrigger(const MemoryTrackerPtr & memory_tracker_, const std::shared_ptr<QueryOperatorSpillContexts> & query_operator_spill_contexts_, float auto_memory_revoke_trigger_threshold, float auto_memory_revoke_target_threshold)
        : memory_tracker(memory_tracker_)
        , query_operator_spill_contexts(query_operator_spill_contexts_)
    {
        RUNTIME_CHECK_MSG(memory_tracker->getLimit() > 0, "Memory limit must be set for auto spill trigger");
        trigger_threshold = static_cast<Int64>(memory_tracker->getLimit() * auto_memory_revoke_trigger_threshold);
        target_threshold = static_cast<Int64>(memory_tracker->getLimit() * auto_memory_revoke_target_threshold);
    }

    void triggerAutoSpill()
    {
        if (memory_tracker->get() > trigger_threshold)
        {
            query_operator_spill_contexts->triggerAutoSpill(memory_tracker->get() - target_threshold);
        }
    }

private:
    MemoryTrackerPtr memory_tracker;
    std::shared_ptr<QueryOperatorSpillContexts> query_operator_spill_contexts;
    Int64 trigger_threshold;
    Int64 target_threshold;
};
} // namespace DB
