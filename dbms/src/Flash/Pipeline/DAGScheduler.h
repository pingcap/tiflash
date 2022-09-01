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

#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineStatusMachine.h>
#include <Flash/Planner/PhysicalPlanNode.h>

namespace DB
{
class PipelineIDGenerator
{
    UInt32 current_id = 0;

public:
    UInt32 nextID()
    {
        return ++current_id;
    }
};

class DAGScheduler
{
public:
    DAGScheduler(
        Context & context_,
        size_t max_streams_,
        const String & req_id)
        : context(context_)
        , max_streams(max_streams_)
        , log(Logger::get("DAGScheduler", req_id))
    {}

    // return <is_success, err_msg>
    std::pair<bool, String> run(
        const PhysicalPlanNodePtr & plan_node,
        ResultHandler result_handler);

    void cancel();

private:
    PipelinePtr genPipeline(const PhysicalPlanNodePtr & plan_node);

    std::unordered_set<UInt32> createParentPipelines(const PhysicalPlanNodePtr & plan_node);

    void submitPipeline(const PipelinePtr & pipeline);

    void submitNext(const PipelinePtr & pipeline);

    void handlePipelineSubmit(
        const PipelineEventPtr & event,
        std::shared_ptr<ThreadManager> & thread_manager);

    void handlePipelineFinish(const PipelineEventPtr & event);

    void handlePipelineFail(const PipelineEventPtr & event, String & err_msg);

    void handlePipelineCancel(const PipelineEventPtr & event);

    void cancelRunningPipelines(bool is_kill);

    PhysicalPlanNodePtr handleResultHandler(
        const PhysicalPlanNodePtr & plan_node,
        ResultHandler result_handler);

private:
    UInt32 final_pipeline_id;

    PipelineStatusMachine status_machine;

    PipelineIDGenerator id_generator;

    MPMCQueue<PipelineEventPtr> event_queue{100};

    Context & context;

    size_t max_streams;

    LoggerPtr log;
};
} // namespace DB
