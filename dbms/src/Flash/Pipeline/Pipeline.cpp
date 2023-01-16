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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Event/Event.h>
#include <Flash/Pipeline/Schedule/Event/PlainPipelineEvent.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Flash/Planner/plans/PhysicalGetResultSink.h>
#include <Flash/Statistics/traverseExecutors.h>
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

void Pipeline::addPlanNode(const PhysicalPlanNodePtr & plan_node)
{
    plan_nodes.push_back(plan_node);
}

void Pipeline::addChild(const PipelinePtr & child)
{
    assert(child);
    children.push_back(child);
}

void Pipeline::toSelfString(FmtBuffer & buffer, size_t level) const
{
    if (level > 0)
        addPrefix(buffer, level).append("|- ");
    buffer.fmtAppend("pipeline#{}: ", id);
    buffer.joinStr(
        plan_nodes.cbegin(),
        plan_nodes.cend(),
        [](const auto & plan_node, FmtBuffer & buf) { buf.append(plan_node->toSimpleString()); },
        " -> ");
}

void Pipeline::toTreeString(FmtBuffer & buffer, size_t level) const
{
    toSelfString(buffer, level);
    if (!children.empty())
        buffer.append("\n");
    ++level;
    for (const auto & child : children)
        child->toTreeString(buffer, level);
}

void Pipeline::addGetResultSink(ResultHandler result_handler)
{
    assert(!plan_nodes.empty());
    auto get_result_sink = PhysicalGetResultSink::build(result_handler, plan_nodes.back());
    addPlanNode(get_result_sink);
}

PipelineExecGroup Pipeline::toExecGroup(Context & context, size_t concurrency)
{
    assert(!plan_nodes.empty());
    PipelineExecGroupBuilder builder;
    for (const auto & plan_node : plan_nodes)
        plan_node->buildPipelineExec(builder, context, concurrency);
    return builder.build();
}

Events Pipeline::toEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency)
{
    Events all_events;
    toEvent(status, context, concurrency, all_events);
    assert(!all_events.empty());
    return all_events;
}

EventPtr Pipeline::toEvent(PipelineExecutorStatus & status, Context & context, size_t concurrency, Events & all_events)
{
    // TODO support fine grained shuffle
    //     - a fine grained partition maps to an event
    //     - the event flow will be
    //     ```
    //     disable fine grained partition pipeline   enable fine grained partition pipeline   enable fine grained partition pipeline
    //                                       ┌───────────────FineGrainedPipelineEvent<────────────────FineGrainedPipelineEvent
    //            PlainPipelineEvent<────────┼───────────────FineGrainedPipelineEvent<────────────────FineGrainedPipelineEvent
    //                                       ├───────────────FineGrainedPipelineEvent<────────────────FineGrainedPipelineEvent
    //                                       └───────────────FineGrainedPipelineEvent<────────────────FineGrainedPipelineEvent
    //     ```
    auto memory_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;

    auto plain_pipeline_event = std::make_shared<PlainPipelineEvent>(status, memory_tracker, context, shared_from_this(), concurrency);
    for (const auto & child : children)
    {
        auto input = child->toEvent(status, context, concurrency, all_events);
        assert(input);
        plain_pipeline_event->addInput(input);
    }
    all_events.push_back(plain_pipeline_event);
    return plain_pipeline_event;
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
