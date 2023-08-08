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
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>
#include <gtest/gtest.h>

#include <vector>

namespace DB::tests
{

namespace
{
class SimpleTask : public Task
{
public:
    explicit SimpleTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
        , task_exec_context(exec_context_)
    {}

    ~SimpleTask() override = default;

    ExecTaskStatus executeImpl() noexcept override
    {
        if (exec_time_counter < total_exec_times)
        {
            ++exec_time_counter;
            std::this_thread::sleep_for(each_exec_time);
            return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::FINISHED;
    }

    void finalizeImpl() override
    {
        task_exec_context.update(profile_info);
    }

    std::chrono::nanoseconds each_exec_time = std::chrono::milliseconds(100);
    uint64_t total_exec_times = 10;
    uint64_t exec_time_counter = 0;
    PipelineExecutorContext & task_exec_context;
};
} // namespace

class TestResourceControlQueue : public ::testing::Test
{
public:
    void SetUp() override
    {
        mem_tracker = MemoryTracker::create(1000000000);
    }

    void TearDown() override
    {
        mem_tracker->reset();
    }

    std::vector<std::shared_ptr<PipelineExecutorContext>> setupExecContextForEachResourceGroup(const std::vector<ResourceGroupPtr> & resource_groups)
    {
        std::vector<std::shared_ptr<PipelineExecutorContext>> all_contexts;
        all_contexts.resize(resource_groups.size());
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            auto resource_group_name = resource_groups[i]->name;
            all_contexts[i] = std::make_shared<PipelineExecutorContext>("mock_query_id-" + resource_group_name, "mock_req_id-" + resource_group_name, mem_tracker, resource_group_name, NullspaceID);
        }
        return all_contexts;
    }

    MemoryTrackerPtr mem_tracker;
};

void nopConsumeResource(const std::string &, const KeyspaceID &, double, uint64_t) {}
double nopGetPriority(const std::string &, const KeyspaceID &)
{
    return 10;
}
bool nopIsResourceGroupThrottled(const std::string &)
{
    return false;
}

void staticConsumeResource(const std::string & name, const KeyspaceID &, double ru, uint64_t cpu_time_ns)
{
    std::lock_guard lock(LocalAdmissionController::global_instance->mu);
    auto & resource_groups = LocalAdmissionController::global_instance->resource_groups;
    auto iter = resource_groups.find(name);
    RUNTIME_ASSERT(iter != resource_groups.end());
    iter->second->consumeResource(ru, cpu_time_ns);
}

double staticGetPriority(const std::string & name, const KeyspaceID &)
{
    std::lock_guard lock(LocalAdmissionController::global_instance->mu);
    auto & resource_groups = LocalAdmissionController::global_instance->resource_groups;
    auto iter = resource_groups.find(name);
    RUNTIME_ASSERT(iter != resource_groups.end());
    auto priority = iter->second->getPriority(LocalAdmissionController::global_instance->max_ru_per_sec);
    return priority;
}

bool staticIsResourceGroupThrottled(const std::string &)
{
    return false;
}

void setupNopLocalAdmissionController()
{
    LocalAdmissionController::global_instance->resetAll();
    LocalAdmissionController::global_instance->consume_resource_func = nopConsumeResource;
    LocalAdmissionController::global_instance->get_priority_func = nopGetPriority;
    LocalAdmissionController::global_instance->is_resource_group_throttled_func = nopIsResourceGroupThrottled;
}

void setupStaticLocalAdmissionController(const std::vector<ResourceGroupPtr> & resource_groups)
{
    LocalAdmissionController::global_instance->resetAll();
    LocalAdmissionController::global_instance->consume_resource_func = staticConsumeResource;
    LocalAdmissionController::global_instance->get_priority_func = staticGetPriority;
    LocalAdmissionController::global_instance->is_resource_group_throttled_func = staticIsResourceGroupThrottled;

    uint64_t max_ru_per_sec = 0;
    for (const auto & resource_group : resource_groups)
    {
        auto cur_ru_per_sec = resource_group->user_ru_per_sec;
        if (max_ru_per_sec < cur_ru_per_sec)
            max_ru_per_sec = cur_ru_per_sec;

        LocalAdmissionController::global_instance->resource_groups.insert({resource_group->name, resource_group});
    }
    LocalAdmissionController::global_instance->max_ru_per_sec = max_ru_per_sec;
}

// Basic test, task runs normally and finish normally.
// Expect runs ok and no error/exception.
TEST_F(TestResourceControlQueue, BasicTest)
{
    setupNopLocalAdmissionController();

    const int thread_num = 10;
    const int resource_group_num = 10;
    const int task_num_per_resource_group = 10;

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    std::vector<TaskPtr> tasks;
    std::vector<std::shared_ptr<PipelineExecutorContext>> all_contexts;
    all_contexts.resize(resource_group_num);
    for (int i = 0; i < resource_group_num; ++i)
    {
        String group_name = "rg" + std::to_string(i);
        all_contexts[i] = std::make_shared<PipelineExecutorContext>("mock-query-id", "mock-req-id", mem_tracker, group_name, NullspaceID);

        for (int j = 0; j < task_num_per_resource_group; ++j)
        {
            auto task = std::make_unique<SimpleTask>(*(all_contexts[i]));
            task->each_exec_time = std::chrono::milliseconds(10);
            task->total_exec_times = 10;
            tasks.push_back(std::move(task));
        }
    }

    task_scheduler.submit(tasks);

    // Expect total cpu_time usage: 10(rg_num) * 10(task_num_per_rg) * 10ms*10 / 10(thead_num)= 1s
    // So it's ok to wait, no need to worry this case running too long.
    for (const auto & context : all_contexts)
        context->wait();
}

// Timeout test, Task need to run 1sec, but we only wait 100ms.
// Expect throw exception.
TEST_F(TestResourceControlQueue, BasicTimeoutTest)
{
    setupNopLocalAdmissionController();

    // NOTE: PipelineExecutorContext need to be destructed after TaskScheduler.
    // Because ~TaskScheduler will call destructor of TaskThreadPool, which will call destructor of Task.
    // In the destructor of Task, will use PipelineExecutorContext to log.
    String group_name = "rg1";
    auto exec_context = std::make_shared<PipelineExecutorContext>("mock-query-id", "mock-req-id", mem_tracker, group_name, NullspaceID);

    auto task = std::make_unique<SimpleTask>(*exec_context);
    task->each_exec_time = std::chrono::milliseconds(100);
    task->total_exec_times = 10;

    const int thread_num = 1;
    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    task_scheduler.submit(std::move(task));
    EXPECT_THROW(exec_context->waitFor(std::chrono::milliseconds(100)), DB::Exception);
}

// 1. resource group runs out of RU, and tasks of this resource group will be stuck.
// 2. When RU is refilled, task can run again.
TEST_F(TestResourceControlQueue, RunOutOfRU)
{
    const std::string rg_name = "rg1";
    // 1. When RU is exhausted, we expect that task cannot be executed.
    const uint64_t ru_per_sec = 0;
    auto resource_group = std::make_shared<ResourceGroup>(rg_name, ResourceGroup::MediumPriorityValue, ru_per_sec, false);
    setupStaticLocalAdmissionController({resource_group});

    const int thread_num = 10;

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    PipelineExecutorContext exec_context("mock-query-id", "mock-req-id", mem_tracker, rg_name, NullspaceID);

    auto task = std::make_unique<SimpleTask>(exec_context);
    // This task should use 2*100ms cpu_time.
    task->total_exec_times = 2;
    task_scheduler.submit(std::move(task));

    // This task is stuck because run out of ru, so will throw timeout exception.
    ASSERT_THROW(exec_context.waitFor(std::chrono::seconds(5)), DB::Exception);

    // Refill RU.
    {
        std::lock_guard lock(LocalAdmissionController::global_instance->mu);
        EXPECT_EQ(LocalAdmissionController::global_instance->resource_groups.erase(rg_name), 1);
        EXPECT_TRUE(LocalAdmissionController::global_instance->resource_groups.empty());
        const uint64_t new_ru_per_sec = 1000000;
        LocalAdmissionController::global_instance->max_ru_per_sec = new_ru_per_sec;
        resource_group = std::make_shared<ResourceGroup>(rg_name, ResourceGroup::MediumPriorityValue, new_ru_per_sec, false);
        LocalAdmissionController::global_instance->resource_groups.insert({rg_name, resource_group});
    }
    // 2. When RU is refilled, the task can be executed again.
    ASSERT_NO_THROW(exec_context.waitFor(std::chrono::seconds(10)));
}

// CPU resource is not enough, and RU is small. Task execution is restricted by RU.
// The proportion of CPU time used by each resource group should be same with the proportion of RU.
TEST_F(TestResourceControlQueue, SamllRUSmallCPU)
{
    // RU proportion is 1:5:10.
    auto resource_groups = std::vector<ResourceGroupPtr>{
        std::make_shared<ResourceGroup>("rg-ru20", ResourceGroup::MediumPriorityValue, 20, false),
        std::make_shared<ResourceGroup>("rg-ru100", ResourceGroup::MediumPriorityValue, 100, false),
        std::make_shared<ResourceGroup>("rg-ru200", ResourceGroup::MediumPriorityValue, 200, false),
    };

    setupStaticLocalAdmissionController(resource_groups);
    auto all_contexts = setupExecContextForEachResourceGroup(resource_groups);

    {
        // NOTE: make task_scheduler inside block, so when we check exec_context.profile_info,
        // tasks will be finialized and execution statistics will be updated.
        const int thread_num = 10;
        const int tasks_per_resource_group = 1000;

        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler(config);

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            for (size_t j = 0; j < tasks_per_resource_group; ++j)
            {
                auto task = std::make_unique<SimpleTask>(*(all_contexts[i]));
                task->each_exec_time = std::chrono::milliseconds(5);
                task->total_exec_times = 100;
                tasks.push_back(std::move(task));
            }
        }

        task_scheduler.submit(tasks);
        std::this_thread::sleep_for(std::chrono::seconds(50));
    }

    for (auto & exec_context : all_contexts)
        std::cout << exec_context->getResourceGroupName() << ": " << exec_context->getQueryProfileInfo().getCPUExecuteTimeNs() << std::endl;

    auto rg1_cpu_time = toCPUTimeMillisecond(all_contexts[0]->getQueryProfileInfo().getCPUExecuteTimeNs());
    auto rg2_cpu_time = toCPUTimeMillisecond(all_contexts[1]->getQueryProfileInfo().getCPUExecuteTimeNs());
    auto rg3_cpu_time = toCPUTimeMillisecond(all_contexts[2]->getQueryProfileInfo().getCPUExecuteTimeNs());

    double rate1 = static_cast<double>(rg2_cpu_time) / rg1_cpu_time;
    EXPECT_TRUE(rate1 >= 3.5 && rate1 <= 6.5);
    double rate2 = static_cast<double>(rg3_cpu_time) / rg1_cpu_time;
    EXPECT_TRUE(rate2 >= 7.5 && rate2 <= 13.5);
}

// Same with TestResourceControlQueue.SmallRUSmallCPU
// TEST_F(TestResourceControlQueue, SmallRULargeCPU)
// {
// }

// RU is large enough, CPU resource is not enough.
// The proportion of CPU time used by each resource group should be same with the proportion of RU.
TEST_F(TestResourceControlQueue, LargeRUSmallCPU)
{
    // RU proportion is 1:5:10.
    auto resource_groups = std::vector<ResourceGroupPtr>{
        std::make_shared<ResourceGroup>("rg-ru20K", ResourceGroup::MediumPriorityValue, 20000, false),
        std::make_shared<ResourceGroup>("rg-ru100K", ResourceGroup::MediumPriorityValue, 100000, false),
        std::make_shared<ResourceGroup>("rg-ru200K", ResourceGroup::MediumPriorityValue, 200000, false),
    };

    setupStaticLocalAdmissionController(resource_groups);
    auto all_contexts = setupExecContextForEachResourceGroup(resource_groups);

    {
        const int thread_num = 10;
        const int tasks_per_resource_group = 1000;

        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler(config);

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            for (size_t j = 0; j < tasks_per_resource_group; ++j)
            {
                auto task = std::make_unique<SimpleTask>(*(all_contexts[i]));
                task->each_exec_time = std::chrono::milliseconds(5);
                task->total_exec_times = 100;
                tasks.push_back(std::move(task));
            }
        }

        task_scheduler.submit(tasks);
        std::this_thread::sleep_for(std::chrono::seconds(50));
    }

    for (auto & exec_context : all_contexts)
        std::cout << exec_context->getResourceGroupName() << ": " << exec_context->getQueryProfileInfo().getCPUExecuteTimeNs() << std::endl;

    auto rg1_cpu_time = toCPUTimeMillisecond(all_contexts[0]->getQueryProfileInfo().getCPUExecuteTimeNs());
    auto rg2_cpu_time = toCPUTimeMillisecond(all_contexts[1]->getQueryProfileInfo().getCPUExecuteTimeNs());
    auto rg3_cpu_time = toCPUTimeMillisecond(all_contexts[2]->getQueryProfileInfo().getCPUExecuteTimeNs());

    double rate1 = static_cast<double>(rg2_cpu_time) / rg1_cpu_time;
    EXPECT_TRUE(rate1 >= 3.5 && rate1 <= 6.5);
    double rate2 = static_cast<double>(rg3_cpu_time) / rg1_cpu_time;
    EXPECT_TRUE(rate2 >= 7.5 && rate2 <= 13.5);
}

// 1. Tasks of different resource groups is restricted by RU, and there is still available cpu resource.
// 2. Change rg-1 as burstable, the available cpu resource is used by rg1.
// 3. Change other rg as burstable, the cpu usage should be same of all resource groups.
TEST_F(TestResourceControlQueue, TestBurstable)
{
}

TEST_F(TestResourceControlQueue, TestAddAndDelResourceGroup)
{
}

} // namespace DB::tests
