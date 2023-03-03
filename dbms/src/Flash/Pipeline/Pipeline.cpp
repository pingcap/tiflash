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
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Events/FineGrainedPipelineEvent.h>
#include <Flash/Pipeline/Schedule/Events/PlainPipelineEvent.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Flash/Planner/Plans/PhysicalGetResultSink.h>
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

void mapEvents(const Events & inputs, const Events & outputs)
{
    assert(!inputs.empty());
    assert(!outputs.empty());
    if (inputs.size() == outputs.size())
    {
        /**
         * 1. for fine grained inputs and fine grained outputs
         *     ```
         *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
         *     ```
         * 2. for non fine grained inputs and non fine grained outputs
         *     ```
         *     PlainPipelineEvent◄────PlainPipelineEvent
         *     ```
         * 3. for non fine grained inputs and fine grained outputs
         *     ```
         *     PlainPipelineEvent◄────FineGrainedPipelineEvent
         *     ```
         * 4. for fine grained inputs and non fine grained outputs
         *     ```
         *     FineGrainedPipelineEvent◄────PlainPipelineEvent
         *     ```
         */
        size_t partition_num = inputs.size();
        for (size_t index = 0; index < partition_num; ++index)
            outputs[index]->addInput(inputs[index]);
    }
    else
    {
        /**
         * 1. for fine grained inputs and fine grained outputs
         *     If the number of partitions does not match, it is safer to use full mapping.
         *     ```
         *     FineGrainedPipelineEvent◄──┐ ┌──FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄──┼─┼──FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄──┤ └──FineGrainedPipelineEvent
         *     FineGrainedPipelineEvent◄──┘
         *     ```
         * 2. for non fine grained inputs and non fine grained outputs
         *     This is not possible, the size of inputs and outputs must be the same and 1.
         * 3. for non fine grained inputs and fine grained outputs
         *     This is not possible, if fine-grained is enabled in outputs, then inputs must also be enabled.
         *     Checked in `doToEvents`.
         * 4. for fine grained inputs and non fine grained outputs
         *     ```
         *                          ┌──FineGrainedPipelineEvent
         *     PlainPipelineEvent◄──┼──FineGrainedPipelineEvent
         *                          ├──FineGrainedPipelineEvent
         *                          └──FineGrainedPipelineEvent
         *     ```
         */
        for (const auto & output : outputs)
        {
            for (const auto & input : inputs)
                output->addInput(input);
        }
    }
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

Block Pipeline::getSampleBlock() const
{
    assert(!plan_nodes.empty());
    return plan_nodes.back()->getSampleBlock();
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

void Pipeline::addGetResultSink(ResultHandler && result_handler)
{
    assert(!plan_nodes.empty());
    auto get_result_sink = PhysicalGetResultSink::build(std::move(result_handler), log, plan_nodes.back());
    addPlanNode(get_result_sink);
}

PipelineExecGroup Pipeline::buildExecGroup(PipelineExecutorStatus & exec_status, Context & context, size_t concurrency)
{
    assert(!plan_nodes.empty());
    PipelineExecGroupBuilder builder;
    for (const auto & plan_node : plan_nodes)
        plan_node->buildPipelineExecGroup(exec_status, builder, context, concurrency);
    return builder.build();
}

bool Pipeline::isFineGrainedPipeline() const
{
    assert(!plan_nodes.empty());
    // The source plan node determines whether the execution mode is fine grained or non-fine grained.
    return plan_nodes.front()->getFineGrainedShuffle().enable();
}

Events Pipeline::toEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency)
{
    Events all_events;
    doToEvents(status, context, concurrency, all_events);
    assert(!all_events.empty());
    return all_events;
}

Events Pipeline::toSelfEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency)
{
    auto memory_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
    Events self_events;
    assert(!plan_nodes.empty());
    if (isFineGrainedPipeline())
    {
        auto fine_grained_exec_group = buildExecGroup(status, context, concurrency);
        assert(!fine_grained_exec_group.empty());
        for (auto & pipeline_exec : fine_grained_exec_group)
            self_events.push_back(std::make_shared<FineGrainedPipelineEvent>(status, memory_tracker, log->identifier(), context, shared_from_this(), std::move(pipeline_exec)));
        LOG_DEBUG(log, "generate {} fine grained pipeline event", self_events.size());
    }
    else
    {
        self_events.push_back(std::make_shared<PlainPipelineEvent>(status, memory_tracker, log->identifier(), context, shared_from_this(), concurrency));
        LOG_DEBUG(log, "generate one plain pipeline event");
    }
    return self_events;
}

Events Pipeline::doToEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency, Events & all_events)
{
    auto self_events = toSelfEvents(status, context, concurrency);
    for (const auto & child : children)
    {
        // If fine-grained is enabled in the current pipeline, then the child must also be enabled.
        RUNTIME_CHECK(!isFineGrainedPipeline() || child->isFineGrainedPipeline());
        auto inputs = child->doToEvents(status, context, concurrency, all_events);
        mapEvents(inputs, self_events);
    }
    all_events.insert(all_events.end(), self_events.cbegin(), self_events.cend());
    return self_events;
}

bool Pipeline::isSupported(const tipb::DAGRequest & dag_request)
{
    bool is_supported = true;
    traverseExecutors(
        &dag_request,
        [&](const tipb::Executor & executor) {
            switch (executor.tp())
            {
            case tipb::ExecType::TypeProjection:
            case tipb::ExecType::TypeSelection:
            case tipb::ExecType::TypeLimit:
            case tipb::ExecType::TypeTopN:
            case tipb::ExecType::TypeTableScan:
            case tipb::ExecType::TypeExchangeSender:
            case tipb::ExecType::TypeExchangeReceiver:
            case tipb::ExecType::TypeExpand:
                return true;
            default:
                is_supported = false;
                return false;
            }
        });
    return is_supported;
}
} // namespace DB
