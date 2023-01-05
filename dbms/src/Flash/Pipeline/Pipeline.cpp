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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/GetResultSink.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineEvent.h>
#include <Flash/Pipeline/PipelineExecStatus.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Operators/OperatorPipelineBuilder.h>
#include <tipb/select.pb.h>

namespace DB
{
namespace
{
FmtBuffer & addPrefix(FmtBuffer & buffer, size_t level)
{
    return buffer.append(String(level, ' '));
}
} // namespace

void Pipeline::addPlan(const PhysicalPlanNodePtr & plan)
{
    plans.push_back(plan);
}

void Pipeline::addDependency(const PipelinePtr & dependency)
{
    dependencies.emplace_back(dependency);
}

void Pipeline::toSelfString(FmtBuffer & buffer, size_t level) const
{
    size_t prefix_size = 2 * level;
    addPrefix(buffer, prefix_size).append("pipeline:\n");
    ++prefix_size;
    for (const auto & plan : plans)
        addPrefix(buffer, prefix_size).append(plan->toString()).append("\n");
}

void Pipeline::toTreeString(FmtBuffer & buffer, size_t level) const
{
    toSelfString(buffer, level);
    ++level;
    for (const auto & dependency : dependencies)
    {
        auto dependency_ptr = dependency.lock();
        assert(dependency_ptr);
        dependency_ptr->toTreeString(buffer, level);
    }
}

void Pipeline::addGetResultSink(ResultHandler result_handler)
{
    assert(!plans.empty());
    auto get_result_sink = PhysicalGetResultSink::build(result_handler, plans.front());
    plans.push_front(get_result_sink);
}

OperatorPipelineGroups Pipeline::transform(Context & context, size_t concurrency)
{
    assert(!plans.empty());
    OperatorPipelineGroupBuilder builder;
    for (auto it = plans.rbegin(); it != plans.rend(); ++it)
        (*it)->transform(builder, context, concurrency);
    return builder.build();
}

Events Pipeline::toEvents(PipelineExecStatus & status, Context & context, size_t concurrency)
{
    Events all_events;
    toEvent(status, context, concurrency, all_events);
    assert(!all_events.empty());
    return all_events;
}

EventPtr Pipeline::toEvent(PipelineExecStatus & status, Context & context, size_t concurrency, Events & all_events)
{
    // TODO support fine grained shuffle
    //     - a fine grained partition maps to an event
    //     - the event flow will be
    //     ```
    //     disable fine grained partition pipeline   enable fine grained partition pipeline   enable fine grained partition pipeline
    //                                       ┌───────────────────PipelineEvent<────────────────────────PipelineEvent
    //               PipelineEvent<──────────┼───────────────────PipelineEvent<────────────────────────PipelineEvent
    //                                       ├───────────────────PipelineEvent<────────────────────────PipelineEvent
    //                                       └───────────────────PipelineEvent<────────────────────────PipelineEvent
    //     ```
    auto memory_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;

    auto pipeline_event = std::make_shared<PipelineEvent>(status, memory_tracker, context, concurrency, shared_from_this());
    for (const auto & dependency : dependencies)
    {
        auto dependency_ptr = dependency.lock();
        assert(dependency_ptr);
        auto dependency_event = dependency_ptr->toEvent(status, context, concurrency, all_events);
        assert(dependency_event);
        pipeline_event->addDependency(dependency_event);
    }
    all_events.push_back(pipeline_event);
    return pipeline_event;
}

bool Pipeline::isSupported(const tipb::DAGRequest & dag_request)
{
    bool is_supported = true;
    traverseExecutors(
        &dag_request,
        [&](const tipb::Executor & executor) {
            if (FineGrainedShuffle(&executor).enable())
            {
                is_supported = false;
                return false;
            }
            switch (executor.tp())
            {
            case tipb::ExecType::TypeProjection:
            case tipb::ExecType::TypeSelection:
            case tipb::ExecType::TypeLimit:
            // Only support mock table_scan/exchange_sender/exchange_receiver in test mode now.
            case tipb::ExecType::TypeTableScan:
            case tipb::ExecType::TypeExchangeSender:
            case tipb::ExecType::TypeExchangeReceiver:
                return true;
            default:
                is_supported = false;
                return false;
            }
        });
    return is_supported;
}
} // namespace DB
