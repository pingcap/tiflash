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

#include <Flash/Pipeline/Schedule/Events/Impls/AggregateFinalConvertEvent.h>
#include <Flash/Pipeline/Schedule/Events/Impls/AggregateFinalSpillEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/AggregateFinalConvertTask.h>
#include <Operators/AggregateContext.h>

namespace DB
{
void AggregateFinalConvertEvent::scheduleImpl()
{
    assert(agg_context);
    for (auto index : indexes)
        addTask(std::make_unique<AggregateFinalConvertTask>(
            exec_context,
            log->identifier(),
            shared_from_this(),
            agg_context,
            index));
}

void AggregateFinalConvertEvent::finishImpl()
{
    if (need_final_spill)
    {
        /// Currently, the aggregation spill algorithm requires all bucket data to be spilled,
        /// so a new event is added here to execute the final spill.
        /// ...──►AggregateBuildSinkOp[local spill]──┐
        /// ...──►AggregateBuildSinkOp[local spill]──┤                                         ┌──►AggregateFinalSpillTask
        /// ...──►AggregateBuildSinkOp[local spill]──┼──►[final spill]AggregateFinalSpillEvent─┼──►AggregateFinalSpillTask
        /// ...──►AggregateBuildSinkOp[local spill]──┤                                         └──►AggregateFinalSpillTask
        /// ...──►AggregateBuildSinkOp[local spill]──┘
        std::vector<size_t> indexes;
        for (size_t index = 0; index < agg_context->getBuildConcurrency(); ++index)
        {
            if (agg_context->needSpill(index, /*try_mark_need_spill=*/true))
                indexes.push_back(index);
        }
        if (!indexes.empty())
        {
            auto final_spill_event = std::make_shared<AggregateFinalSpillEvent>(
                exec_context,
                log->identifier(),
                agg_context,
                std::move(indexes),
                std::move(profile_infos));
            insertEvent(final_spill_event);
        }
    }

    auto dur = getFinishDuration();
    for (const auto & profile_info : profile_infos)
        profile_info->execution_time += dur;
    agg_context.reset();
}
} // namespace DB
