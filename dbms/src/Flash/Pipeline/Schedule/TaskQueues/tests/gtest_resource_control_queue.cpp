// Copyright 2023 PingCAP, Ltd.
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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>

#include <gtest/gtest.h>
#include <vector>

namespace DB::tests
{

namespace
{
class PlainTask : public Task
{
public:
    explicit PlainTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_) {}

    ExecTaskStatus executeImpl() noexcept override
    {
        if (exec_time_counter < total_exec_times)
        {
            ++exec_time_counter;
            std::this_thread::sleep_for(each_exec_time_in_ns);
        }
        return ExecTaskStatus::FINISHED;
    }

    std::chrono::nanoseconds each_exec_time_in_ns = std::chrono::nanoseconds(100);
    uint64_t total_exec_times = 100;
    uint64_t exec_time_counter = 0;
};
}  // namespace

class TestResourceControlQueue : public ::testing::Test
{
};

TEST_F(TestResourceControlQueue, BasicTest)
{
    TaskQueuePtr queue = std::make_unique<ResourceControlQueue>();

    const int thread_num = 10;
    const int task_num = 100;

    PipelineExecutorContext exec_context;

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    std::vector<TaskPtr> tasks;
    for (int i = 0; i < task_num; ++i)
        tasks.push_back(std::make_unique<PlainTask>(exec_context));

    task_scheduler.submit(tasks);

    exec_context.query_profile_info.getCPUExecuteTimeNs();
}

} // namespace DB::test
