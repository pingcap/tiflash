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

#include <Flash/Pipeline/task/EventLoop.h>
#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>
#include <Flash/Pipeline/dag/Event.h>

namespace DB
{
EventLoop::EventLoop(size_t loop_id_, PipelineManager & pipeline_manager_)
    : loop_id(loop_id_)
    , pipeline_manager(pipeline_manager_)
{}

void EventLoop::submit(PipelineTask && task)
{
    RUNTIME_ASSERT(
        event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
        "EventLoop event queue full");
}

void EventLoop::finish()
{
    event_queue.finish();
}

void EventLoop::handleSubmit(PipelineTask & task)
{
    auto result = task.execute(loop_id);
    switch (result.type)
    {
    case PipelineTaskResultType::running:
    {
        RUNTIME_ASSERT(
            event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
            "EventLoop event queue full");
        break;
    }
    case PipelineTaskResultType::finished:
    {
        if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); dag_scheduler)
        {
            dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
        }
        break;
    }
    case PipelineTaskResultType::error:
    {
        if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); dag_scheduler)
        {
            dag_scheduler->submit(PipelineEvent::fail(result.err_msg));
        }
        break;
    }
    default:
        break;
    }
}

void EventLoop::loop()
{
    PipelineTask task;
    while (event_queue.pop(task) == MPMCQueueResult::OK)
    {
        handleSubmit(task);
    }
}
} // namespace DB
