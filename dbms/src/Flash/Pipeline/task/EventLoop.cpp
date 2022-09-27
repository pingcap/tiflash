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

#include <Common/MemoryTrackerSetter.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>
#include <Flash/Pipeline/dag/Event.h>
#include <Flash/Pipeline/task/EventLoop.h>
#include <errno.h>

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
    MemoryTrackerSetter setter(true, task.getMemTracker());
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
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(loop_id, &cpu_set);
    int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
    if (ret != 0)
        throw Exception(fmt::format("sched_setaffinity fail: {}", std::strerror(errno)));
#endif
    setThreadName(fmt::format("event loop(%s)", loop_id).c_str());

    PipelineTask task;
    while (event_queue.pop(task) == MPMCQueueResult::OK)
    {
        handleSubmit(task);
    }
}
} // namespace DB
