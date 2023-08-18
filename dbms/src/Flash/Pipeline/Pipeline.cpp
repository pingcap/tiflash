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
} // namespace

PipelineEvents::PipelineEvents(Events && events_, bool is_fine_grained_)
    : events(std::move(events_))
    , is_fine_grained(is_fine_grained_)
{
    RUNTIME_CHECK(!events.empty());
    // For non fine grained mode, the size of events must be 1.
    RUNTIME_CHECK(is_fine_grained || events.size() == 1);
}

void PipelineEvents::mapInputs(const PipelineEvents & inputs)
{
    /// The self events is output.
    if (inputs.is_fine_grained && is_fine_grained)
    {
        if (inputs.events.size() == events.size())
        {
            /**
             * 1. If the number of partitions match, use fine grained mapping here.
             *     ```
             *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄────FineGrainedPipelineEvent
             *     ```
             */
            size_t partition_num = inputs.events.size();
            for (size_t index = 0; index < partition_num; ++index)
                events[index]->addInput(inputs.events[index]);
        }
        else
        {
            /**
             * 2. If the number of partitions does not match, it is safer to use full mapping.
             *     ```
             *     FineGrainedPipelineEvent◄──┐ ┌──FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄──┼─┼──FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄──┤ └──FineGrainedPipelineEvent
             *     FineGrainedPipelineEvent◄──┘
             *     ```
             */
            for (const auto & output : events)
            {
                for (const auto & input : inputs.events)
                    output->addInput(input);
            }
        }
    }
    else
    {
        /**
         * Use full mapping here.
         * 1. for non fine grained inputs and non fine grained outputs
         *     The size of inputs and outputs must be the same and 1.
         *     ```
         *     PlainPipelineEvent◄────PlainPipelineEvent
         *     ```
         * 2. for non fine grained inputs and fine grained outputs
         *     This is not possible, if fine-grained is enabled in outputs, then inputs must also be enabled.
         * 3. for fine grained inputs and non fine grained outputs
         *     ```
         *                          ┌──FineGrainedPipelineEvent
         *     PlainPipelineEvent◄──┼──FineGrainedPipelineEvent
         *                          ├──FineGrainedPipelineEvent
         *                          └──FineGrainedPipelineEvent
         * 
         *     PlainPipelineEvent◄────FineGrainedPipelineEvent
         *     ```
         */

        // If the outputs is fine grained model, the intputs must also be.
        RUNTIME_CHECK(inputs.is_fine_grained || !is_fine_grained);
        for (const auto & output : events)
        {
            for (const auto & input : inputs.events)
                output->addInput(input);
        }
    }
}

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

void Pipeline::addGetResultSink(const ResultQueuePtr & result_queue)
{
    assert(!plan_nodes.empty());
    auto get_result_sink = PhysicalGetResultSink::build(result_queue, log, plan_nodes.back());
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

/**
 * There are two execution modes in pipeline.
 * 1. non fine grained mode
 *     A pipeline generates an event(PlainPipelineEvent).
 *     This means that all the operators in the pipeline are finished before the next pipeline is triggered.
 * 2. fine grained mode
 *     A pipeline will generate n Events(FineGrainedPipelineEvent), one for each data partition.
 *     There is a fine-grained mapping of Events between Pipelines, e.g. only Events from the same data partition will have dependencies on each other.
 *     This means that once some data partition of the previous pipeline has finished, the operators of the next pipeline's corresponding data partition can be started without having to wait for the entire pipeline to finish.
 * 
 *        ┌──non fine grained mode──┐                          ┌──fine grained mode──┐
 *                                            ┌──FineGrainedPipelineEvent◄───FineGrainedPipelineEvent
 * PlainPipelineEvent◄───PlainPipelineEvent◄──┼──FineGrainedPipelineEvent◄───FineGrainedPipelineEvent
 *                                            └──FineGrainedPipelineEvent◄───FineGrainedPipelineEvent
 */
bool Pipeline::isFineGrainedMode() const
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

PipelineEvents Pipeline::toSelfEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency)
{
    auto memory_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
    Events self_events;
    assert(!plan_nodes.empty());
    if (isFineGrainedMode())
    {
        auto fine_grained_exec_group = buildExecGroup(status, context, concurrency);
        for (auto & pipeline_exec : fine_grained_exec_group)
            self_events.push_back(std::make_shared<FineGrainedPipelineEvent>(status, memory_tracker, log->identifier(), std::move(pipeline_exec)));
        LOG_DEBUG(log, "Execute in fine grained model and generate {} fine grained pipeline event", self_events.size());
    }
    else
    {
        self_events.push_back(std::make_shared<PlainPipelineEvent>(status, memory_tracker, log->identifier(), context, shared_from_this(), concurrency));
        LOG_DEBUG(log, "Execute in non fine grained model and generate one plain pipeline event");
    }
    return {std::move(self_events), isFineGrainedMode()};
}

PipelineEvents Pipeline::doToEvents(PipelineExecutorStatus & status, Context & context, size_t concurrency, Events & all_events)
{
    auto self_events = toSelfEvents(status, context, concurrency);
    for (const auto & child : children)
        self_events.mapInputs(child->doToEvents(status, context, concurrency, all_events));
    all_events.insert(all_events.end(), self_events.events.cbegin(), self_events.events.cend());
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
            case tipb::ExecType::TypeTableScan:
                // TODO support keep order.
                is_supported = !executor.tbl_scan().keep_order();
                return is_supported;
            case tipb::ExecType::TypeProjection:
            case tipb::ExecType::TypeSelection:
            case tipb::ExecType::TypeLimit:
            case tipb::ExecType::TypeTopN:
            case tipb::ExecType::TypeExchangeSender:
            case tipb::ExecType::TypeExchangeReceiver:
            case tipb::ExecType::TypeExpand:
            case tipb::ExecType::TypeAggregation:
                return true;
            case tipb::ExecType::TypeWindow:
            case tipb::ExecType::TypeSort:
                // TODO support non fine grained shuffle.
                is_supported = FineGrainedShuffle(&executor).enable();
                return is_supported;
            default:
                is_supported = false;
                return false;
            }
        });
    return is_supported;
}
} // namespace DB
