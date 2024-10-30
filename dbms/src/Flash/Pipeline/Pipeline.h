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

#pragma once

#include <Flash/Executor/ResultHandler.h>
#include <Flash/Executor/ResultQueue_fwd.h>
#include <Flash/Pipeline/Exec/PipelineExec.h>

#include <deque>
#include <vector>

namespace tipb
{
class DAGRequest;
}

namespace DB
{
struct Settings;

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;

class Event;
using EventPtr = std::shared_ptr<Event>;
using Events = std::vector<EventPtr>;

class PhysicalPlanNode;
using PhysicalPlanNodePtr = std::shared_ptr<PhysicalPlanNode>;

class PipelineExecutorContext;

struct PipelineEvents
{
    Events events;
    bool is_fine_grained;

    PipelineEvents(Events && events_, bool is_fine_grained_);

    void mapInputs(const PipelineEvents & inputs);
};

class Pipeline : public std::enable_shared_from_this<Pipeline>
{
public:
    Pipeline(UInt32 id_, const String & req_id)
        : id(id_)
        , log(Logger::get(req_id, id_))
    {}

    void addPlanNode(const PhysicalPlanNodePtr & plan_node);

    void addChild(const PipelinePtr & child);

    const String & toTreeString() const;

    // used for getting the result blocks.
    void addGetResultSink(const ResultQueuePtr & result_queue);

    PipelineExecGroup buildExecGroup(PipelineExecutorContext & exec_context, Context & context, size_t concurrency);

    Events toEvents(PipelineExecutorContext & exec_context, Context & context, size_t concurrency);

    Block getSampleBlock() const;

    bool isFineGrainedMode() const;

    /// This method will not be called for fine grained pipeline.
    /// This method is used to execute two-stage logic and is not suitable for fine grained execution mode,
    /// such as local/global join build and local/final agg spill.
    ///  ┌─stage1─┐      ┌─stage2─┐
    ///     task1──┐    ┌──►task1
    ///     task2──┼──►─┼──►task2
    ///     ...    │    │   ...
    ///     taskn──┘    └──►taskm
    EventPtr complete(PipelineExecutorContext & exec_context);

    String getFinalPlanExecId() const;

    void setHasPipelineBreakerWaitTime(bool value) { has_pipeline_breaker_wait_time = value; }

private:
    void toTreeStringImpl(FmtBuffer & buffer, size_t level) const;
    void toSelfString(FmtBuffer & buffer, size_t level) const;

    PipelineEvents toSelfEvents(PipelineExecutorContext & exec_context, Context & context, size_t concurrency);
    PipelineEvents doToEvents(
        PipelineExecutorContext & exec_context,
        Context & context,
        size_t concurrency,
        Events & all_events);

private:
    const UInt32 id;
    LoggerPtr log;

    bool is_fine_grained_mode = true;

    // data flow: plan_nodes.begin() --> plan_nodes.end()
    std::deque<PhysicalPlanNodePtr> plan_nodes;

    std::vector<PipelinePtr> children;

    mutable String tree_string;
    bool has_pipeline_breaker_wait_time;
};
} // namespace DB
