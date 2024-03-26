// Copyright 2023 PingCAP, Inc.
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
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>
#include <gtest/gtest.h>

#include <random>
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

    ReturnStatus executeImpl() noexcept override
    {
        if (exec_time_counter < total_exec_times)
        {
            ++exec_time_counter;
            std::this_thread::sleep_for(each_exec_time);
            return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::FINISHED;
    }

    void finalizeImpl() override { task_exec_context.update(profile_info); }

    std::chrono::nanoseconds each_exec_time = std::chrono::milliseconds(100);
    uint64_t total_exec_times = 10;
    uint64_t exec_time_counter = 0;
    PipelineExecutorContext & task_exec_context;
};
} // namespace

std::shared_ptr<ResourceGroup> createResourceGroupOfStaticTokenBucket(
    const std::string & rg_name,
    int32_t user_priority,
    uint64_t user_ru_per_sec,
    bool burstable)
{
    return std::make_shared<ResourceGroup>(rg_name, user_priority, user_ru_per_sec, burstable);
}

std::shared_ptr<ResourceGroup> createResourceGroupOfDynamicTokenBucket(
    const std::string & rg_name,
    int32_t user_priority,
    uint64_t user_ru_per_sec,
    bool burstable)
{
    auto resource_group = std::make_shared<ResourceGroup>(rg_name, user_priority, user_ru_per_sec, burstable);

    // Default token bucket is static.
    // Here we setup dynamic token bucket.
    TokenBucket::TokenBucketConfig config(
        user_ru_per_sec,
        user_ru_per_sec,
        static_cast<double>(std::numeric_limits<uint64_t>::max()));
    resource_group->bucket->reConfig(config);
    return resource_group;
}

void dynamicConsumeResource(const std::string & name, double ru, uint64_t cpu_time_ns)
{
    std::lock_guard lock(LocalAdmissionController::global_instance->mu);
    auto & resource_groups = LocalAdmissionController::global_instance->resource_groups;
    auto iter = resource_groups.find(name);
    RUNTIME_ASSERT(iter != resource_groups.end());
    iter->second->consumeResource(ru, cpu_time_ns);
}

uint64_t dynamicGetPriority(const std::string & name)
{
    std::lock_guard lock(LocalAdmissionController::global_instance->mu);
    auto & resource_groups = LocalAdmissionController::global_instance->resource_groups;
    auto iter = resource_groups.find(name);
    RUNTIME_ASSERT(iter != resource_groups.end());
    auto priority = iter->second->getPriority(LocalAdmissionController::global_instance->max_ru_per_sec);
    return priority;
}

void setupNopLAC()
{
    LocalAdmissionController::global_instance.reset();
    LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
    LocalAdmissionController::global_instance->consume_resource_func = nopConsumeResource;
    LocalAdmissionController::global_instance->get_priority_func = nopGetPriority;
}

void setupMockLAC(const std::vector<ResourceGroupPtr> & resource_groups)
{
    LocalAdmissionController::global_instance.reset();
    LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
    LocalAdmissionController::global_instance->consume_resource_func = dynamicConsumeResource;
    LocalAdmissionController::global_instance->get_priority_func = dynamicGetPriority;

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

std::vector<TaskPtr> setupTasks(
    std::vector<std::shared_ptr<PipelineExecutorContext>> & all_contexts,
    size_t tasks_per_resource_group,
    size_t task_exec_ms,
    size_t task_exec_count)
{
    std::vector<TaskPtr> tasks;
    // Assume length of all_contexts equals resource group num.
    for (auto & exec_context : all_contexts)
    {
        for (size_t i = 0; i < tasks_per_resource_group; ++i)
        {
            auto task = std::make_unique<SimpleTask>(*exec_context);
            task->each_exec_time = std::chrono::milliseconds(task_exec_ms);
            task->total_exec_times = task_exec_count;
            tasks.push_back(std::move(task));
        }
    }
    return tasks;
}

class TestResourceControlQueue : public ::testing::Test
{
public:
    void SetUp() override { mem_tracker = MemoryTracker::create(1000000000); }

    void TearDown() override { mem_tracker->reset(); }

    std::vector<std::shared_ptr<PipelineExecutorContext>> setupExecContextForEachResourceGroup(
        const std::vector<ResourceGroupPtr> & resource_groups,
        const String & query_id_prefix = "",
        const String & req_id_prefix = "")
    {
        std::vector<std::shared_ptr<PipelineExecutorContext>> all_contexts;
        all_contexts.resize(resource_groups.size());
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            auto resource_group_name = resource_groups[i]->name;
            all_contexts[i] = std::make_shared<PipelineExecutorContext>(
                query_id_prefix + resource_group_name,
                req_id_prefix + resource_group_name,
                mem_tracker,
                nullptr,
                nullptr,
                resource_group_name);
        }
        return all_contexts;
    }

    double testSmallRULargeCPU(bool static_token_bucket, bool rg1_burstable)
    {
        const uint64_t rg1_ru_per_sec = 50;
        const uint64_t rg2_ru_per_sec = 100;
        std::vector<ResourceGroupPtr> resource_groups;
        if (static_token_bucket)
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfStaticTokenBucket(
                    "rg1",
                    ResourceGroup::MediumPriorityValue,
                    rg1_ru_per_sec,
                    rg1_burstable),
                createResourceGroupOfStaticTokenBucket(
                    "rg2",
                    ResourceGroup::MediumPriorityValue,
                    rg2_ru_per_sec,
                    false),
            };
        else
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfDynamicTokenBucket(
                    "rg1",
                    ResourceGroup::MediumPriorityValue,
                    rg1_ru_per_sec,
                    rg1_burstable),
                createResourceGroupOfDynamicTokenBucket(
                    "rg2",
                    ResourceGroup::MediumPriorityValue,
                    rg2_ru_per_sec,
                    false),
            };
        const uint64_t exec_dura_sec = 5;

        setupMockLAC(resource_groups);
        auto all_contexts = setupExecContextForEachResourceGroup(resource_groups);

        const size_t tasks_per_resource_group = 1;
        const size_t task_exec_ms = 50;
        const size_t task_exec_count = 100;
        auto tasks = setupTasks(all_contexts, tasks_per_resource_group, task_exec_ms, task_exec_count);

        {
            const int thread_num = 10;
            TaskSchedulerConfig config{thread_num, thread_num};
            TaskScheduler task_scheduler(config);
            task_scheduler.submit(tasks);

            std::this_thread::sleep_for(std::chrono::seconds(exec_dura_sec));
        }

        auto rg1_cpu_time = toCPUTimeMillisecond(all_contexts[0]->getQueryProfileInfo().getCPUExecuteTimeNs());
        auto rg2_cpu_time = toCPUTimeMillisecond(all_contexts[1]->getQueryProfileInfo().getCPUExecuteTimeNs());
        double rate = static_cast<double>(rg2_cpu_time) / rg1_cpu_time;

        std::cout << "rg1_cpu_time " << rg1_cpu_time << std::endl;
        std::cout << "rg2_cpu_time " << rg2_cpu_time << std::endl;
        std::cout << "rate " << rate << std::endl;

        return rate;
    }

    void testSmallRUSmallCPU(bool static_token_bucket)
    {
        std::vector<ResourceGroupPtr> resource_groups;
        if (static_token_bucket)
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfStaticTokenBucket("rg-ru20", ResourceGroup::MediumPriorityValue, 20, false),
                createResourceGroupOfStaticTokenBucket("rg-ru100", ResourceGroup::MediumPriorityValue, 100, false),
                createResourceGroupOfStaticTokenBucket("rg-ru200", ResourceGroup::MediumPriorityValue, 200, false),
            };
        else
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfDynamicTokenBucket("rg-ru20", ResourceGroup::MediumPriorityValue, 20, false),
                createResourceGroupOfDynamicTokenBucket("rg-ru100", ResourceGroup::MediumPriorityValue, 100, false),
                createResourceGroupOfDynamicTokenBucket("rg-ru200", ResourceGroup::MediumPriorityValue, 200, false),
            };

        setupMockLAC(resource_groups);
        auto all_contexts = setupExecContextForEachResourceGroup(resource_groups);

        const int tasks_per_resource_group = 1000;
        const size_t task_exec_ms = 5;
        const size_t task_exec_count = 100;
        std::vector<TaskPtr> tasks = setupTasks(all_contexts, tasks_per_resource_group, task_exec_ms, task_exec_count);

        {
            // NOTE: make task_scheduler inside block, so when we check exec_context.profile_info,
            // tasks will be finialized and execution statistics will be updated.
            const int thread_num = 3;

            TaskSchedulerConfig config{thread_num, thread_num};
            TaskScheduler task_scheduler(config);

            task_scheduler.submit(tasks);
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }

        for (auto & exec_context : all_contexts)
            std::cout << exec_context->getResourceGroupName() << ": "
                      << exec_context->getQueryProfileInfo().getCPUExecuteTimeNs() << std::endl;

        auto rg1_cpu_time = toCPUTimeMillisecond(all_contexts[0]->getQueryProfileInfo().getCPUExecuteTimeNs());
        auto rg2_cpu_time = toCPUTimeMillisecond(all_contexts[1]->getQueryProfileInfo().getCPUExecuteTimeNs());
        auto rg3_cpu_time = toCPUTimeMillisecond(all_contexts[2]->getQueryProfileInfo().getCPUExecuteTimeNs());

        double rate1 = static_cast<double>(rg2_cpu_time) / rg1_cpu_time;
        double rate2 = static_cast<double>(rg3_cpu_time) / rg1_cpu_time;
        double rate3 = static_cast<double>(rg3_cpu_time) / rg2_cpu_time;
        std::cout << "rate1: " << rate1 << std::endl;
        std::cout << "rate2: " << rate2 << std::endl;
        std::cout << "rate3: " << rate3 << std::endl;

        EXPECT_TRUE(rate1 > 1);
        EXPECT_TRUE(rate2 > 1);
        EXPECT_TRUE(rate3 > 1);
    }

    void testLargeRUSmallCPU(bool static_token_bucket)
    {
        std::vector<ResourceGroupPtr> resource_groups;
        // RU proportion is 1:5:10.
        if (static_token_bucket)
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfStaticTokenBucket("rg-ru20K", ResourceGroup::MediumPriorityValue, 20000, false),
                createResourceGroupOfStaticTokenBucket("rg-ru100K", ResourceGroup::MediumPriorityValue, 100000, false),
                createResourceGroupOfStaticTokenBucket("rg-ru200K", ResourceGroup::MediumPriorityValue, 200000, false),
            };
        else
            resource_groups = std::vector<ResourceGroupPtr>{
                createResourceGroupOfDynamicTokenBucket("rg-ru20K", ResourceGroup::MediumPriorityValue, 20000, false),
                createResourceGroupOfDynamicTokenBucket("rg-ru100K", ResourceGroup::MediumPriorityValue, 100000, false),
                createResourceGroupOfDynamicTokenBucket("rg-ru200K", ResourceGroup::MediumPriorityValue, 200000, false),
            };

        setupMockLAC(resource_groups);
        auto all_contexts = setupExecContextForEachResourceGroup(resource_groups);
        const size_t task_exec_ms = 5;
        const size_t task_exec_count = 100;
        const int tasks_per_resource_group = 1000;
        std::vector<TaskPtr> tasks = setupTasks(all_contexts, tasks_per_resource_group, task_exec_ms, task_exec_count);

        {
            const int thread_num = 3;

            TaskSchedulerConfig config{thread_num, thread_num};
            TaskScheduler task_scheduler(config);

            task_scheduler.submit(tasks);
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }

        for (auto & exec_context : all_contexts)
            std::cout << exec_context->getResourceGroupName() << ": "
                      << exec_context->getQueryProfileInfo().getCPUExecuteTimeNs() << std::endl;

        auto rg1_cpu_time = toCPUTimeMillisecond(all_contexts[0]->getQueryProfileInfo().getCPUExecuteTimeNs());
        auto rg2_cpu_time = toCPUTimeMillisecond(all_contexts[1]->getQueryProfileInfo().getCPUExecuteTimeNs());
        auto rg3_cpu_time = toCPUTimeMillisecond(all_contexts[2]->getQueryProfileInfo().getCPUExecuteTimeNs());

        double rate1 = static_cast<double>(rg2_cpu_time) / rg1_cpu_time;
        double rate2 = static_cast<double>(rg3_cpu_time) / rg1_cpu_time;

        std::cout << "rate1: " << rate1 << std::endl;
        std::cout << "rate2: " << rate2 << std::endl;

        EXPECT_TRUE(rate1 >= 3.5 && rate1 <= 6.5);
        EXPECT_TRUE(rate2 >= 7.5 && rate2 <= 13.5);
    }

    MemoryTrackerPtr mem_tracker;
};

// Basic test, task runs normally and finish normally.
// Expect runs ok and no error/exception.
TEST_F(TestResourceControlQueue, BasicTest)
{
    setupNopLAC();

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
        all_contexts[i] = std::make_shared<PipelineExecutorContext>(
            "mock-query-id",
            "mock-req-id",
            mem_tracker,
            nullptr,
            nullptr,
            group_name);

        for (int j = 0; j < task_num_per_resource_group; ++j)
        {
            auto task = std::make_unique<SimpleTask>(*(all_contexts[i]));
            task->each_exec_time = std::chrono::milliseconds(10);
            task->total_exec_times = 10;
            tasks.push_back(std::move(task));
        }
    }

    task_scheduler.submit(tasks);

    // Expect total cpu_time usage: 10(rg_num) * 10(task_num_per_rg) * 10ms*10 / 10(thead_num)= 1s.
    // So it's ok to wait, no need to worry this case running too long.
    for (const auto & context : all_contexts)
        context->waitFor(std::chrono::seconds(20));
}

// Timeout test, Task needs to run 1sec, but we only wait 100ms.
// Expect throw exception.
TEST_F(TestResourceControlQueue, BasicTimeoutTest)
{
    setupNopLAC();

    // NOTE: PipelineExecutorContext need to be destructed after TaskScheduler.
    // Because ~TaskScheduler() will call destructor of TaskThreadPool, which will call destructor of Task.
    // In the destructor of Task, will use PipelineExecutorContext to log.
    String group_name = "rg1";
    auto exec_context = std::make_shared<PipelineExecutorContext>(
        "mock-query-id",
        "mock-req-id",
        mem_tracker,
        nullptr,
        nullptr,
        group_name);

    auto task = std::make_unique<SimpleTask>(*exec_context);
    task->each_exec_time = std::chrono::milliseconds(100);
    task->total_exec_times = 10;

    const int thread_num = 1;
    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    task_scheduler.submit(std::move(task));
    EXPECT_THROW(exec_context->waitFor(std::chrono::milliseconds(100)), DB::Exception);
}

TEST_F(TestResourceControlQueue, RunOutOfRU)
{
    const std::string rg_name = "rg1";
    // 1. When RU is exhausted, expect that task cannot be executed.
    const uint64_t ru_per_sec = 1;
    auto resource_group
        = createResourceGroupOfDynamicTokenBucket(rg_name, ResourceGroup::MediumPriorityValue, ru_per_sec, false);
    setupMockLAC({resource_group});

    const int thread_num = 10;

    TaskSchedulerConfig config{thread_num, thread_num};
    TaskScheduler task_scheduler(config);

    PipelineExecutorContext exec_context("mock-query-id", "mock-req-id", mem_tracker, nullptr, nullptr, rg_name);

    auto task = std::make_unique<SimpleTask>(exec_context);
    // This task should use 5*100ms cpu_time.
    task->total_exec_times = 5;
    task_scheduler.submit(std::move(task));

    // This task is stuck because run out of ru, so will throw timeout exception.
    ASSERT_THROW(exec_context.waitFor(std::chrono::seconds(5)), DB::Exception);

    // 2. When RU is refilled, the task can be executed again.
    {
        std::lock_guard lock(LocalAdmissionController::global_instance->mu);
        EXPECT_EQ(LocalAdmissionController::global_instance->resource_groups.erase(rg_name), 1);
        EXPECT_TRUE(LocalAdmissionController::global_instance->resource_groups.empty());
        const uint64_t new_ru_per_sec = 1000000;
        resource_group = createResourceGroupOfDynamicTokenBucket(
            rg_name,
            ResourceGroup::MediumPriorityValue,
            new_ru_per_sec,
            false);
        LocalAdmissionController::global_instance->resource_groups.insert({rg_name, resource_group});
        LocalAdmissionController::global_instance->max_ru_per_sec = new_ru_per_sec;
    }
    LocalAdmissionController::global_instance->refill_token_callback();
    ASSERT_NO_THROW(exec_context.waitFor(std::chrono::seconds(10)));
}

// RU is very small, so task execution is restricted by RU.
// The proportion of CPU time of each resource group should be same with the proportion of RU.
TEST_F(TestResourceControlQueue, SmallRULargeCPUStaticTokenBucket)
{
    auto rate = testSmallRULargeCPU(true, false);
    // Expect rate is 2, but it's affected by ci env. So only expect greater than 1.
    EXPECT_TRUE(rate > 1);
}
TEST_F(TestResourceControlQueue, SmallRULargeCPUDynamicTokenBucket)
{
    auto rate = testSmallRULargeCPU(false, false);
    EXPECT_TRUE(rate > 1);
}

// CPU resource is not enough, and RU is small.
// It is difficult to get an accurate proportion of the execution time of two RGs,
// becase task execution is restricted by both RU and cpu resource.
// So only check resource group with bigger RU will use more cpu.
TEST_F(TestResourceControlQueue, SmallRUSmallCPUStaticTokenBucket)
{
    testSmallRUSmallCPU(true);
}
TEST_F(TestResourceControlQueue, SmallRUSmallCPUDynamicTokenBucket)
{
    testSmallRUSmallCPU(false);
}

// RU is large enough, CPU resource is not enough.
// The proportion of CPU time of each resource group should be same with the proportion of RU.
TEST_F(TestResourceControlQueue, LargeRUSmallCPUStaticTokenBucket)
{
    testLargeRUSmallCPU(true);
}
TEST_F(TestResourceControlQueue, LargeRUSmallCPUDynamicTokenBucket)
{
    testLargeRUSmallCPU(false);
}

// Both RU and cpu resource is enough.
// Expect all tasks should run ok, and RCQ should effect the execution of tasks.
// Maybe too trivial, no need to test.
// TEST_F(TestResourceControlQueue, LargeRULargeCPU) {}

// Make resource group with small RU to be burstable.
// Expect it use more cpu than resource group with large RU.
TEST_F(TestResourceControlQueue, TestBurstableStaticTokenBucket)
{
    auto rate = testSmallRULargeCPU(true, true);
    EXPECT_TRUE(rate < 1);
}
TEST_F(TestResourceControlQueue, TestBurstableDynamicTokenBucket)
{
    auto rate = testSmallRULargeCPU(false, true);
    EXPECT_TRUE(rate < 1);
}

// Test priority queue of ResourceControlQueue: Less priority value means higher priority.
TEST_F(TestResourceControlQueue, ResourceControlPriorityQueueTest)
{
    setupNopLAC();

    std::random_device dev;
    std::mt19937 gen(dev());
    std::uniform_int_distribution dist;

    const int count = 1000;
    std::vector<uint64_t> priority_vals;
    priority_vals.resize(count);
    for (int i = 0; i < count; ++i)
    {
        priority_vals[i] = dist(gen);
    }
    for (int i = 0; i < 10; ++i)
    {
        // some special priority value.
        // 0 is not possible, but we still test it.
        // uint64_max means RU is exhausted, it's the lowest priority.
        priority_vals.push_back(0);
        priority_vals.push_back(std::numeric_limits<uint64_t>::max());
    }

    ResourceControlQueue<CPUMultiLevelFeedbackQueue> test_queue;
    for (size_t i = 0; i < priority_vals.size(); ++i)
    {
        test_queue.resource_group_infos.push({"rg-" + std::to_string(i), priority_vals[i], nullptr});
    }

    std::sort(priority_vals.begin(), priority_vals.end());

    for (auto priority : priority_vals)
    {
        const auto & info = test_queue.resource_group_infos.top();
        EXPECT_EQ(info.priority, priority);
        test_queue.resource_group_infos.pop();
    }
    EXPECT_TRUE(test_queue.resource_group_infos.empty());
}

TEST_F(TestResourceControlQueue, cancel)
{
    const auto rg_names = std::vector<String>{"rg-ru20K", "rg-ru100K", "rg-ru200K"};
    auto resource_groups = std::vector<ResourceGroupPtr>{
        createResourceGroupOfDynamicTokenBucket(rg_names[0], ResourceGroup::MediumPriorityValue, 20000, false),
        createResourceGroupOfDynamicTokenBucket(rg_names[1], ResourceGroup::MediumPriorityValue, 100000, false),
        createResourceGroupOfDynamicTokenBucket(rg_names[2], ResourceGroup::MediumPriorityValue, 200000, false),
    };
    const String query_id_prefix = "mock_query_id";
    const String req_id_prefix = "mock_req_id";
    const int tasks_per_resource_group = 1000;

    setupMockLAC(resource_groups);

    auto setup_tasks
        = [&](std::vector<std::shared_ptr<PipelineExecutorContext>> & all_contexts, std::vector<TaskPtr> & tasks) {
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
          };

    // submit then cancel.
    {
        std::vector<TaskPtr> tasks;
        auto all_contexts = setupExecContextForEachResourceGroup(resource_groups, query_id_prefix, req_id_prefix);
        setup_tasks(all_contexts, tasks);

        ResourceControlQueue<CPUMultiLevelFeedbackQueue> queue;
        queue.submit(tasks);
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            queue.cancel(query_id_prefix + rg_names[i], rg_names[i]);
            for (size_t j = 0; j < tasks_per_resource_group; ++j)
            {
                TaskPtr task;
                queue.take(task);
                EXPECT_EQ(task->getQueryId(), query_id_prefix + rg_names[i]);
                FINALIZE_TASK(task);
            }
        }
    }

    // cancel then submit.
    {
        std::vector<TaskPtr> tasks;
        auto all_contexts = setupExecContextForEachResourceGroup(resource_groups, query_id_prefix, req_id_prefix);
        setup_tasks(all_contexts, tasks);

        ResourceControlQueue<CPUMultiLevelFeedbackQueue> queue;
        for (size_t i = 0; i < resource_groups.size(); ++i)
        {
            queue.cancel(query_id_prefix + rg_names[i], rg_names[i]);
            if (i == 0)
                queue.submit(tasks);

            for (size_t j = 0; j < tasks_per_resource_group; ++j)
            {
                TaskPtr task;
                queue.take(task);
                EXPECT_EQ(task->getQueryId(), query_id_prefix + rg_names[i]);
                FINALIZE_TASK(task);
            }
        }
    }
}

TEST_F(TestResourceControlQueue, tokenBucket)
{
    const double fill_rate = 10.0;
    const double init_tokens = 10.0;
    {
        TokenBucket bucket(fill_rate, init_tokens, "log_id");
        for (int i = 0; i < 10; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            bucket.peek();
        }
        ASSERT_GT(bucket.peek(), init_tokens);
        ASSERT_GE(bucket.peek(), init_tokens + fill_rate * 10 * std::chrono::milliseconds(10).count() / 1000);
    }
    {
        TokenBucket bucket(fill_rate, init_tokens, "log_id");
        for (int i = 0; i < 1000; ++i)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(5));
            bucket.peek();
        }
        ASSERT_GT(bucket.peek(), init_tokens);
        ASSERT_GE(bucket.peek(), init_tokens + fill_rate * 1000 * std::chrono::microseconds(5).count() / 1000000);
    }
}

} // namespace DB::tests
