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

#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Flash/Pipeline/DAGScheduler.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/plans/PhysicalJoin.h>

namespace DB
{
void DAGScheduler::run(const PhysicalPlanNodePtr & plan_node)
{
    assert(plan_node);
    auto final_pipeline = genPipeline(plan_node);
    final_pipeline_id = final_pipeline->getId();
    submitPipeline(final_pipeline);

    auto thread_manager = newThreadManager();

    bool is_running = true;
    while(is_running)
    {
        PipelineEventPtr event;
        if (event_queue.pop(event))
        {
            assert(event);
            switch(event->type)
            {
            case PipelineEventType::submit:
            {
                thread_manager->schedule(true, "ExecutePipeline", [&, event]() {
                    auto pipeline = event->pipeline;
                    pipeline->execute(context, max_streams);
                    status_machine.stateToComplete(pipeline->getId());
                    event_queue.push(PipelineEvent::finish(pipeline));
                });
                break;
            }
            case PipelineEventType::finish:
            {
                if (status_machine.isCompleted(final_pipeline_id))
                {
                    // query finish
                    is_running = false;
                }
                else
                {
                    const auto & finish_pipeline = event->pipeline;
                    submitNext(finish_pipeline);
                }
                break;
            }
            default:
                throw Exception("Unknown event type");
            }
        }
        else
        {
            is_running = false;
        }
    }
    thread_manager->wait();
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
}
