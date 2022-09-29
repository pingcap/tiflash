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
#include <Common/ThreadManager.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Pipeline/task/EventLoop.h>
#include <Server/ServerInfo.h>

#include <functional>
#include <random>

namespace DB
{
struct PipelineManager;

class TaskScheduler
{
public:
    TaskScheduler(PipelineManager & pipeline_manager_, const ServerInfo & server_info);

    ~TaskScheduler();

    void submit(std::vector<PipelineTask> & tasks);

    void cancel(UInt32 pipeline_id);

    size_t concurrency() const;

private:
    // numa nodes<logical cpus>
    std::vector<std::vector<EventLoopPtr>> numa_event_loops;
    size_t total_event_loop_num = 0;

    std::shared_ptr<ThreadPoolManager> thread_pool_manager;

    std::mt19937 gen;
    std::uniform_int_distribution<size_t> numa_dis;
    std::vector<std::uniform_int_distribution<size_t>> event_loop_dis;

    LoggerPtr log = Logger::get("TaskScheduler");
};
} // namespace DB
