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
#include <Flash/Pipeline/PhysicalResultHandler.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalJoin.h>
#include <Flash/Planner/plans/PhysicalJoinProbe.h>

namespace DB
{
std::pair<bool, String> DAGScheduler::run(
    const PhysicalPlanNodePtr & plan_node,
    ResultHandler result_handler)
{
    assert(plan_node);
    auto final_pipeline = genPipeline(handleResultHandler(plan_node, result_handler));
    final_pipeline_id = final_pipeline->getId();
    LOG_FMT_DEBUG(log, "pipeline dag:\n{}", pipelineDAGToString(final_pipeline_id));

    submitPipeline(final_pipeline);

    auto thread_manager = newThreadManager();

    PipelineEvent event;
    String err_msg;
    while (event_queue.pop(event) == MPMCQueueResult::OK)
    {
        switch (event.type)
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

String DAGScheduler::pipelineDAGToString(UInt32 pipeline_id) const
{
    FmtBuffer fb;
    auto pipeline = status_machine.getPipeline(pipeline_id);
    fb.fmtAppend("id: {}, parents: [{}]\n", pipeline_id, fmt::join(pipeline->getParentIds(), ", "));
    for (auto parent_id : pipeline->getParentIds())
        fb.append(pipelineDAGToString(parent_id));
    return fb.toString();
}

PhysicalPlanNodePtr DAGScheduler::handleResultHandler(
    const PhysicalPlanNodePtr & plan_node,
    ResultHandler result_handler)
{
    return PhysicalResultHandler::build(result_handler, log->identifier(), plan_node);
}

void DAGScheduler::cancel(bool is_kill)
{
    event_queue.push(PipelineEvent::cancel(is_kill));
}

void DAGScheduler::handlePipelineCancel(const PipelineEvent & event)
{
    assert(event.type == PipelineEventType::cancel);
    event_queue.cancel();
    cancelRunningPipelines(event.is_kill);
    status_machine.finish();
}

void DAGScheduler::cancelRunningPipelines(bool is_kill)
{
    for (const auto & pipeline : status_machine.getRunningPipelines())
        pipeline->cancel(is_kill);
}

void DAGScheduler::handlePipelineFail(const PipelineEvent & event, String & err_msg)
{
    assert(event.type == PipelineEventType::fail);
    if (event.pipeline)
        status_machine.stateToComplete(event.pipeline->getId());
    err_msg = event.err_msg;
    event_queue.cancel();
    cancelRunningPipelines(false);
    status_machine.finish();
}

void DAGScheduler::handlePipelineFinish(const PipelineEvent & event)
{
    assert(event.type == PipelineEventType::finish && event.pipeline);
    status_machine.stateToComplete(event.pipeline->getId());
    if (status_machine.isCompleted(final_pipeline_id))
    {
        event_queue.finish();
        status_machine.finish();
    }
    else
    {
        const auto & finish_pipeline = event.pipeline;
        submitNext(finish_pipeline);
    }
}

void DAGScheduler::handlePipelineSubmit(
    const PipelineEvent & event,
    std::shared_ptr<ThreadManager> & thread_manager)
{
    assert(event.type == PipelineEventType::submit && event.pipeline);
    auto pipeline = event.pipeline;
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
    return createNonJoinedPipelines(pipeline);
}

PipelinePtr DAGScheduler::createNonJoinedPipelines(const PipelinePtr & pipeline)
{
    std::vector<std::pair<size_t, PhysicalPlanNodePtr>> non_joined;
    size_t index = 0;
    PhysicalPlanVisitor::visit(pipeline->getPlanNode(), [&](const PhysicalPlanNodePtr & plan) {
        assert(plan);
        if (plan->tp() == PlanType::JoinProbe)
        {
            auto physical_join_probe = std::static_pointer_cast<PhysicalJoinProbe>(plan);
            if (auto ret = physical_join_probe->splitNonJoinedPlanNode(); ret.has_value())
                non_joined.emplace_back(index, *ret);
        }
        ++index;
        return true;
    });

    auto gen_plan_tree = [&](PhysicalPlanNodePtr root, size_t index, const PhysicalPlanNodePtr & leaf) -> PhysicalPlanNodePtr {
        assert(root && leaf);
        if (index == 0)
            return leaf;
        root = root->cloneOne();
        root->notTiDBOperator();
        PhysicalPlanNodePtr parent = root;
        assert(parent->childrenSize() == 1);
        for (size_t i = 0; i < index - 1; ++i)
        {
            auto pre = parent;
            parent = pre->children(0);
            assert(parent->childrenSize() == 1);
            parent = parent->cloneOne();
            parent->notTiDBOperator();
            pre->setChild(0, parent);
        }
        parent->setChild(0, leaf);
        return root;
    };

    std::unordered_set<UInt32> parent_pipelines;
    parent_pipelines.insert(pipeline->getId());
    PipelinePtr return_pipeline = pipeline;
    for (int i = non_joined.size() - 1; i >= 0; --i)
    {
        auto [index, non_joined_plan] = non_joined[i];
        auto id = id_generator.nextID();
        auto non_joined_root = gen_plan_tree(pipeline->getPlanNode(), index, non_joined_plan);
        auto non_joined_pipeline = std::make_shared<Pipeline>(non_joined_root, id, parent_pipelines, log->identifier());
        status_machine.addPipeline(non_joined_pipeline);
        parent_pipelines.insert(id);
        return_pipeline = non_joined_pipeline;
    }
    return return_pipeline;
}

std::unordered_set<UInt32> DAGScheduler::createParentPipelines(const PhysicalPlanNodePtr & plan_node)
{
    std::unordered_set<UInt32> parent_ids;
    for (size_t i = 0; i < plan_node->childrenSize(); ++i)
    {
        const auto & child = plan_node->children(i);
        switch (child->tp())
        {
        case PlanType::Join:
        {
            // PhysicalJoin cannot be the root node.
            auto physical_join = std::static_pointer_cast<PhysicalJoin>(child);
            // pipeline breaker: PhysicalJoinBuild
            parent_ids.insert(genPipeline(physical_join->build())->getId());

            // remove PhysicalJoin
            plan_node->setChild(0, physical_join->probe());
            const auto & ids = createParentPipelines(physical_join->probe());
            parent_ids.insert(ids.cbegin(), ids.cend());
            break;
        }
        case PlanType::Aggregation:
        {
            // PhysicalAggregation cannot be the root node.
            auto physical_agg = std::static_pointer_cast<PhysicalAggregation>(child);
            // pipeline breaker: PhysicalPartialAggregation
            parent_ids.insert(genPipeline(physical_agg->partial())->getId());

            // remove PhysicalAggregation
            plan_node->setChild(0, physical_agg->final());
            const auto & ids = createParentPipelines(physical_agg->final());
            parent_ids.insert(ids.cbegin(), ids.cend());
            break;
        }
        default:
        {
            const auto & ids = createParentPipelines(child);
            parent_ids.insert(ids.cbegin(), ids.cend());
        }
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
