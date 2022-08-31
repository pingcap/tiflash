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

#include <Common/CPUAffinityManager.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Flash/Pipeline/DAGScheduler.h>
#include <Flash/Pipeline/ResultHandlerPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/plans/PhysicalJoin.h>

namespace DB
{
std::pair<bool, String> DAGScheduler::run(
    const PhysicalPlanNodePtr & plan_node, 
    ResultHandler result_handler)
{
    assert(plan_node);
    auto final_pipeline = genPipeline(handleResultHandler(plan_node, result_handler));
    final_pipeline_id = final_pipeline->getId();
    submitPipeline(final_pipeline);

    auto thread_manager = newThreadManager();

    PipelineEventPtr event;
    String err_msg;
    while (event_queue.pop(event) == MPMCQueueResult::OK)
    {
        assert(event);
        switch (event->type)
        {
        case PipelineEventType::submit:
            handlePipelineSubmit(event, thread_manager);
            break;
        case PipelineEventType::finish:
            handlePipelineFinish(event);
            break;
        case PipelineEventType::fail:
            handlePipelineFail(event, err_msg);
            break;
        case PipelineEventType::cancel:
            handlePipelineCancel(event);
            break;
        default:
            event_queue.push(PipelineEvent::fail("Unknown event type"));
        }
    }
    thread_manager->wait();
    return {event_queue.getStatus() == MPMCQueueStatus::FINISHED, err_msg};
}

void DAGScheduler::cancel()
{
    event_queue.push(PipelineEvent::cancel());
}

void DAGScheduler::handlePipelineCancel(const PipelineEventPtr & event)
{
    assert(event && event->type == PipelineEventType::cancel);
    event_queue.cancel();
    cancelRunningPipelines(true);
    status_machine.finish();
}

void DAGScheduler::cancelRunningPipelines(bool is_kill)
{
    for (const auto & pipeline : status_machine.getRunningPipelines())
        pipeline->cancel(is_kill);
}

void DAGScheduler::handlePipelineFail(const PipelineEventPtr & event, String & err_msg)
{
    assert(event && event->type == PipelineEventType::fail);
    if (event->pipeline)
        status_machine.stateToComplete(event->pipeline->getId());
    err_msg = event->err_msg;
    event_queue.cancel();
    cancelRunningPipelines(false);
    status_machine.finish();
}

PhysicalPlanNodePtr DAGScheduler::handleResultHandler(
    const PhysicalPlanNodePtr & plan_node, 
    ResultHandler result_handler)
{
    return result_handler.isDefault()
        ? plan_node
        : PhysicalResultHandler::build(result_handler, log->identifier(), plan_node);
}

void DAGScheduler::handlePipelineFinish(const PipelineEventPtr & event)
{
    assert(event && event->type == PipelineEventType::finish && event->pipeline);
    status_machine.stateToComplete(event->pipeline->getId());
    if (status_machine.isCompleted(final_pipeline_id))
    {
        event_queue.finish();
        status_machine.finish();
    }
    else
    {
        const auto & finish_pipeline = event->pipeline;
        submitNext(finish_pipeline);
    }
}

void DAGScheduler::handlePipelineSubmit(
    const PipelineEventPtr & event,
    std::shared_ptr<ThreadManager> & thread_manager)
{
    assert(event && event->type == PipelineEventType::submit && event->pipeline);
    auto pipeline = event->pipeline;
    pipeline->prepare(context, max_streams);
    thread_manager->schedule(true, "ExecutePipeline", [&, pipeline]() {
        CPUAffinityManager::getInstance().bindSelfQueryThread();
        try
        {
            pipeline->execute();
            event_queue.push(PipelineEvent::finish(pipeline));
        }
        catch (...)
        {
            auto err_msg = getCurrentExceptionMessage(true, true);
            event_queue.push(PipelineEvent::fail(pipeline, err_msg));
        }
    });
}

PipelinePtr DAGScheduler::genPipeline(const PhysicalPlanNodePtr & plan_node)
{
    const auto & parent_ids = createParentPipelines(plan_node);
    auto id = id_generator.nextID();
    auto pipeline = std::make_shared<Pipeline>(plan_node, id, parent_ids, log->identifier());
    status_machine.addPipeline(pipeline);
    return pipeline;
}

std::unordered_set<UInt32> DAGScheduler::createParentPipelines(const PhysicalPlanNodePtr & plan_node)
{
    std::unordered_set<UInt32> parent_ids;
    for (size_t i = 0; i < plan_node->childrenSize(); ++i)
    {
        const auto & child = plan_node->children(i);
        if (child->tp() == PlanType::Join)
        {
            auto physical_join = std::static_pointer_cast<PhysicalJoin>(child);
            // pipeline breaker: PhysicalJoinBuild
            parent_ids.insert(genPipeline(physical_join->build())->getId());

            const auto & ids = createParentPipelines(physical_join->probe());
            parent_ids.insert(ids.cbegin(), ids.cend());
        }
        else
        {
            const auto & ids = createParentPipelines(child);
            parent_ids.insert(ids.cbegin(), ids.cend());
        }
    }
    return parent_ids;
}

void DAGScheduler::submitPipeline(const PipelinePtr & pipeline)
{
    assert(pipeline);

    if (status_machine.isRunning(pipeline->getId()) || status_machine.isCompleted(pipeline->getId()))
        return;

    bool is_ready_for_run = true;
    for (const auto & parent_id : pipeline->getParentIds())
    {
        if (!status_machine.isCompleted(parent_id))
        {
            is_ready_for_run = false;
            submitPipeline(status_machine.getPipeline(parent_id));
        }
    }

    if (is_ready_for_run)
    {
        status_machine.stateToRunning(pipeline->getId());
        event_queue.push(PipelineEvent::submit(pipeline));
    }
    else
    {
        status_machine.stateToWaiting(pipeline->getId());
    }
}

void DAGScheduler::submitNext(const PipelinePtr & pipeline)
{
    const auto & next_pipelines = status_machine.nextPipelines(pipeline->getId());
    assert(!next_pipelines.empty());
    for (const auto & next_pipeline : next_pipelines)
        submitPipeline(next_pipeline);
}
} // namespace DB
