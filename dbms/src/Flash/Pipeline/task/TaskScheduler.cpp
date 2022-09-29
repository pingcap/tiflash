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
#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
TaskScheduler::TaskScheduler(PipelineManager & pipeline_manager, const ServerInfo & server_info)
{
    auto numa_nodes = DM::getNumaNodes(log->getLog());
    LOG_FMT_INFO(log, "numa_nodes {} => {}", numa_nodes.size(), numa_nodes);
    if (numa_nodes.size() == 1 && numa_nodes.back().empty())
    {
        int logical_cores = server_info.cpu_info.logical_cores;
        RUNTIME_ASSERT(logical_cores > 0);
        std::vector<EventLoopPtr> event_loops;
        event_loops.reserve(logical_cores);
        for (int core = 0; core < logical_cores; ++core)
            event_loops.emplace_back(std::make_unique<EventLoop>(core, pipeline_manager));
        total_event_loop_num = event_loops.size();
        numa_event_loops.emplace_back(std::move(event_loops));
    }
    else
    {
        total_event_loop_num = 0;
        for (const auto & node : numa_nodes)
        {
            RUNTIME_ASSERT(!node.empty());
            std::vector<EventLoopPtr> event_loops;
            event_loops.reserve(node.size());
            for (auto core : node)
                event_loops.emplace_back(std::make_unique<EventLoop>(core, pipeline_manager));
            total_event_loop_num += event_loops.size();
            numa_event_loops.emplace_back(std::move(event_loops));
        }
    }

    thread_pool_manager = newThreadPoolManager(total_event_loop_num);
    for (const auto & event_loops : numa_event_loops)
    {
        for (const auto & event_loop : event_loops)
        {
            // TODO 2 thread for per event loop.
            thread_pool_manager->schedule(false, [&]() {
                event_loop->loop();
            });
        }
    }
    LOG_DEBUG(log, "init {} event loop success", total_event_loop_num);

    std::random_device rd;
    gen = std::mt19937{rd()};
    numa_dis = std::uniform_int_distribution<size_t>(0, numa_event_loops.size() - 1);
    event_loop_dis.reserve(numa_event_loops.size());
    for (const auto & event_loops : numa_event_loops)
        event_loop_dis.emplace_back(std::uniform_int_distribution<size_t>(0, event_loops.size() - 1));
}

TaskScheduler::~TaskScheduler()
{
    for (const auto & event_loops : numa_event_loops)
    {
        for (const auto & event_loop : event_loops)
            event_loop->finish();
    }
    thread_pool_manager->wait();
    numa_event_loops.clear();
}

void TaskScheduler::submit(std::vector<PipelineTask> & tasks)
{
    size_t i = 0;
    while ((tasks.size() - i) >= total_event_loop_num)
    {
        for (const auto & event_loops : numa_event_loops)
        {
            for (const auto & event_loop : event_loops)
                event_loop->submit(std::move(tasks[i++]));
        }
    }

    auto next_numa_id = [&]() {
        static size_t j = numa_dis(gen);
        size_t numa_id = j++;
        j %= numa_event_loops.size();
        return numa_id;
    };
    while (i < tasks.size())
    {
        auto numa_id = next_numa_id();
        auto & numa = numa_event_loops[numa_id];

        if ((tasks.size() - i) >= numa.size())
        {
            for (const auto & event_loop : numa)
                event_loop->submit(std::move(tasks[i++]));
        }
        else
        {
            auto next_loop = [&]() -> EventLoop & {
                static size_t k = event_loop_dis[numa_id](gen);
                EventLoop & loop = *numa[k++];
                k %= numa.size();
                return loop;
            };
            while (i < tasks.size())
                next_loop().submit(std::move(tasks[i++]));
        }
    }
}

size_t TaskScheduler::concurrency() const
{
    return total_event_loop_num;
}
} // namespace DB
