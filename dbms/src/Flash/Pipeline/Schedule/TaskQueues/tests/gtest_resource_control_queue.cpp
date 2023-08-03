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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>

#include <gtest/gtest.h>
#include <vector>

namespace DB
{
auto LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
}

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
            std::this_thread::sleep_for(each_exec_time);
        }
        return ExecTaskStatus::FINISHED;
    }

    std::chrono::nanoseconds each_exec_time = std::chrono::milliseconds(100);
    uint64_t total_exec_times = 100;
    uint64_t exec_time_counter = 0;
};
}  // namespace

class TestResourceControlQueue : public ::testing::Test
{
};


TEST_F(TestResourceControlQueue, BasicTest)
{
    TaskQueuePtr queue = std::make_unique<ResourceControlQueue<CPUMultiLevelFeedbackQueue>>();

    const int thread_num = 10;
    const int resource_group_num = 10;
    const int task_num_per_resource_group = 100;

    const int init_cpu_usage = 0;
    const int init_remaining_ru = 1000000000;

    auto mem_tracker = MemoryTracker::create(1000000000);

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    std::vector<TaskPtr> tasks;
    std::vector<std::shared_ptr<PipelineExecutorContext>> all_contexts;
    all_contexts.resize(resource_group_num);
    for (int i = 0; i < resource_group_num; ++i)
    {
        String group_name = "rg-" + std::to_string(i);
        all_contexts[i] = std::make_shared<PipelineExecutorContext>("mock-query-id", "mock-req-id", mem_tracker, group_name, NullspaceID);

        LocalAdmissionController::global_instance->resource_groups.insert({group_name, {init_cpu_usage, init_remaining_ru}});
        for (int j = 0; j < task_num_per_resource_group; ++j)
        {
            tasks.push_back(std::make_unique<PlainTask>(*(all_contexts[i])));
        }
    }

    task_scheduler.submit(tasks);
    
    for (const auto & context : all_contexts)
        context->waitFor(std::chrono::seconds(20));
}

// When RU is exhausted, we expect that task cannot be executed.
TEST_F(TestResourceControlQueue, RunOutOFRU)
{
    const int thread_num = 10;

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    const String group_name = "rg1";
    LocalAdmissionController::global_instance->resource_groups.insert({group_name, {0, 0}});

    auto mem_tracker = MemoryTracker::create(1000000000);
    PipelineExecutorContext exec_context("mock-query-id", "mock-req-id", mem_tracker, group_name, NullspaceID);

    auto task = std::make_unique<PlainTask>(exec_context);
    task_scheduler.submit(std::move(task));

    try
    {
        exec_context.waitFor(std::chrono::seconds(10));
    }
    catch (...)
    {
        String err_msg = getCurrentExceptionMessage(false, false);
        EXPECT_EQ(PipelineExecutorContext::timeout_err_msg, err_msg);
    }
}

// 1. When RU is exhausted, task is stopped.
// 2. When RU is refilled, the task can be executed again.
TEST_F(TestResourceControlQueue, RefillRU)
{
}

// The proportion of CPU time used by each resource group should be same with the proportion of RU.
TEST_F(TestResourceControlQueue, CPUUsageProportion)
{
}

} // namespace DB::test
