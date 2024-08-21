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
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Events/Impls/FineGrainedPipelineEvent.h>
#include <Flash/Pipeline/Schedule/Events/Impls/PlainPipelineEvent.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Flash/Planner/Plans/PhysicalGetResultSink.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Settings.h>
#include <tipb/select.pb.h>

#include <magic_enum.hpp>

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

        // If the outputs is fine grained mode, the inputs must also be.
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
    assert(plan_node);
    /// For fine grained mode, all plan node should enable fine grained shuffle.
    if (!plan_node->getFineGrainedShuffle().enabled())
        is_fine_grained_mode = false;
    plan_nodes.push_back(plan_node);
}

void Pipeline::addChild(const PipelinePtr & child)
{
    RUNTIME_CHECK(child);
    children.push_back(child);
}

Block Pipeline::getSampleBlock() const
{
    RUNTIME_CHECK(!plan_nodes.empty());
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

const String & Pipeline::toTreeString() const
{
    if (!tree_string.empty())
        return tree_string;

    FmtBuffer buffer;
    toTreeStringImpl(buffer, 0);
    tree_string = buffer.toString();
    return tree_string;
}

void Pipeline::toTreeStringImpl(FmtBuffer & buffer, size_t level) const
{
    toSelfString(buffer, level);
    if (!children.empty())
        buffer.append("\n");
    ++level;
    for (const auto & child : children)
        child->toTreeStringImpl(buffer, level);
}

void Pipeline::addGetResultSink(const ResultQueuePtr & result_queue)
{
    RUNTIME_CHECK(!plan_nodes.empty());
    auto get_result_sink = PhysicalGetResultSink::build(result_queue, log, plan_nodes.back());
    addPlanNode(get_result_sink);
}

String Pipeline::getFinalPlanExecId() const
{
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (auto it = plan_nodes.crbegin(); it != plan_nodes.crend(); ++it)
    {
        const auto & plan_node = *it;
        if (plan_node->isTiDBOperator())
            return plan_node->execId();
    }
    return "";
}

PipelineExecGroup Pipeline::buildExecGroup(
    PipelineExecutorContext & exec_context,
    Context & context,
    size_t concurrency)
{
    RUNTIME_CHECK(!plan_nodes.empty());
    PipelineExecGroupBuilder builder;
    for (const auto & plan_node : plan_nodes)
    {
        plan_node->buildPipelineExecGroup(exec_context, builder, context, concurrency);
    }
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
    return is_fine_grained_mode;
}

EventPtr Pipeline::complete(PipelineExecutorContext & exec_context)
{
    assert(!isFineGrainedMode());
    if unlikely (exec_context.isCancelled())
        return nullptr;
    assert(!plan_nodes.empty());
    return plan_nodes.back()->sinkComplete(exec_context);
}

Events Pipeline::toEvents(PipelineExecutorContext & exec_context, Context & context, size_t concurrency)
{
    Events all_events;
    doToEvents(exec_context, context, concurrency, all_events);
    RUNTIME_CHECK(!all_events.empty());
    return all_events;
}

PipelineEvents Pipeline::toSelfEvents(PipelineExecutorContext & exec_context, Context & context, size_t concurrency)
{
    Events self_events;
    RUNTIME_CHECK(!plan_nodes.empty());
    if (isFineGrainedMode())
    {
        auto fine_grained_exec_group = buildExecGroup(exec_context, context, concurrency);
        for (auto & pipeline_exec : fine_grained_exec_group)
            self_events.push_back(
                std::make_shared<FineGrainedPipelineEvent>(exec_context, log->identifier(), std::move(pipeline_exec)));
        LOG_DEBUG(log, "Execute in fine grained mode and generate {} fine grained pipeline event", self_events.size());
    }
    else
    {
        self_events.push_back(std::make_shared<PlainPipelineEvent>(
            exec_context,
            log->identifier(),
            context,
            shared_from_this(),
            concurrency));
        LOG_DEBUG(log, "Execute in non fine grained mode and generate one plain pipeline event");
    }
    return {std::move(self_events), isFineGrainedMode()};
}

PipelineEvents Pipeline::doToEvents(
    PipelineExecutorContext & exec_context,
    Context & context,
    size_t concurrency,
    Events & all_events)
{
    auto self_events = toSelfEvents(exec_context, context, concurrency);
    for (const auto & child : children)
        self_events.mapInputs(child->doToEvents(exec_context, context, concurrency, all_events));
    all_events.insert(all_events.end(), self_events.events.cbegin(), self_events.events.cend());
    return self_events;
}
} // namespace DB
