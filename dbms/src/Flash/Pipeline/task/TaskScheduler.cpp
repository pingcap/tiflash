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

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
TaskScheduler::TaskScheduler(PipelineManager & pipeline_manager)
{
    auto cores = getNumberOfPhysicalCPUCores();
    thread_pool_manager = newThreadPoolManager(cores);
    event_loops.reserve(cores);
    for (size_t index = 0; index < cores; ++index)
    {
        event_loops.emplace_back(std::make_shared<EventLoop>(index, pipeline_manager));
    }
    for (size_t index = 0; index < cores; ++index)
    {
        thread_pool_manager->schedule(false, [&, index]() {
            event_loops[index]->loop();
        });
    }
}

TaskScheduler::~TaskScheduler()
{
    for (const auto & event_loop : event_loops)
        event_loop->finish();
    event_loops.clear();
    thread_pool_manager->wait();
}

void TaskScheduler::submit(std::vector<PipelineTask> & tasks)
{
    size_t i = 0;
    auto next_loop = [&]() -> EventLoop & {
        EventLoop & loop = *event_loops[i++];
        i %= event_loops.size();
        return loop;
    };
    for (auto & task : tasks)
    {
        next_loop().submit(std::move(task));
    }
}

size_t TaskScheduler::concurrency() const
{
    return event_loops.size();
}
} // namespace DB
