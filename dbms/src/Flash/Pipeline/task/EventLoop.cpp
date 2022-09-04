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

void EventLoop::submit(TaskEvent && event)
{
    RUNTIME_ASSERT(
        event_queue.tryPush(std::move(event)) != MPMCQueueResult::FULL,
        "EventLoop event queue full");
}

void EventLoop::finish()
{
    event_queue.finish();
}

void EventLoop::handleSubmit(TaskEvent & event)
{
    auto result = event.task.execute(loop_id);
    switch (result.status)
    {
    case PipelineTaskStatus::running:
    {
        RUNTIME_ASSERT(
            event_queue.tryPush(std::move(event)) != MPMCQueueResult::FULL,
            "EventLoop event queue full");
        break;
    }
    case PipelineTaskStatus::finished:
    {
        auto dag_scheduler = pipeline_manager.getDAGScheduler(event.task.mpp_task_id);
        assert(dag_scheduler);
        dag_scheduler->submit(PipelineEvent::finish(event.task.task_id, event.task.pipeline_id));
        break;
    }
    case PipelineTaskStatus::error:
    {
        // todo
        break;
    }
    default:
        break;
    }
}

void EventLoop::loop()
{
    TaskEvent event;
    while (event_queue.pop(event) == MPMCQueueResult::OK)
    {
        switch (event.type)
        {
        case TaskEventType::submit:
            handleSubmit(event);
            break;
        case TaskEventType::cancel:
        {
            // todo
            break;
        }
        default:
            break;
        }
    }
}
} // namespace DB
