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
void AggregateFinalSpillEvent::scheduleImpl()
{
    assert(agg_context);
    for (auto index : indexes)
        addTask(std::make_unique<AggregateFinalSpillTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), agg_context, index));
}

void AggregateFinalSpillEvent::finishImpl()
{
    auto dur = getFinishDuration();
    for (const auto & profile_info : profile_infos)
        profile_info->execution_time += dur;
    agg_context.reset();
}
} // namespace DB
