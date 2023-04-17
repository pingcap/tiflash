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

#include <Flash/Pipeline/Schedule/Events/AggregateFinalSpillEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/AggregateFinalSpillTask.h>

namespace DB
{
std::vector<TaskPtr> AggregateFinalSpillEvent::scheduleImpl()
{
    assert(agg_context);
    std::vector<TaskPtr> tasks;
    tasks.reserve(indexes.size());
    for (auto index : indexes)
        tasks.push_back(std::make_unique<AggregateFinalSpillTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), agg_context, index));
    return tasks;
}

void AggregateFinalSpillEvent::finishImpl()
{
    agg_context.reset();
}
} // namespace DB
