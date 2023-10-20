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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class SimpleTask : public Task
{
public:
    explicit SimpleTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {}

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 5;
};

class SimpleWaitingTask : public Task
{
public:
    explicit SimpleWaitingTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {}

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
                return ExecTaskStatus::WAITING;
            else
            {
                --loop_count;
                return ExecTaskStatus::RUNNING;
            }
        }
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus awaitImpl() noexcept override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
            {
                --loop_count;
                return ExecTaskStatus::WAITING;
            }
            else
                return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 10 + random() % 10;
};

class SimpleBlockedTask : public Task
{
public:
    explicit SimpleBlockedTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {}

protected:
    ExecTaskStatus executeImpl() override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
                return ExecTaskStatus::IO_IN;
            else
            {
                --loop_count;
                return ExecTaskStatus::RUNNING;
            }
        }
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus executeIOImpl() override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
            {
                --loop_count;
                return ExecTaskStatus::IO_IN;
            }
            else
                return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 10 + random() % 10;
};

class MemoryTraceTask : public Task
{
public:
    explicit MemoryTraceTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {
        assert(exec_context_.getMemoryTracker() != nullptr);
    }

    // From CurrentMemoryTracker::MEMORY_TRACER_SUBMIT_THRESHOLD
    static constexpr Int64 MEMORY_TRACER_SUBMIT_THRESHOLD = 1024 * 1024; // 1 MiB

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        CurrentMemoryTracker::alloc(MEMORY_TRACER_SUBMIT_THRESHOLD - 10);
        return ExecTaskStatus::IO_IN;
    }

    ExecTaskStatus executeIOImpl() override
    {
        CurrentMemoryTracker::alloc(MEMORY_TRACER_SUBMIT_THRESHOLD + 10);
        return ExecTaskStatus::WAITING;
    }

    ExecTaskStatus awaitImpl() override
    {
        // await wouldn't call MemoryTracker.
        return ExecTaskStatus::FINISHED;
    }
};

class DeadLoopTask : public Task
{
public:
    explicit DeadLoopTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {}

protected:
    ExecTaskStatus executeImpl() override { return ExecTaskStatus::WAITING; }

    ExecTaskStatus awaitImpl() override { return ExecTaskStatus::IO_IN; }

    ExecTaskStatus executeIOImpl() override { return ExecTaskStatus::RUNNING; }
};
} // namespace

class TaskSchedulerTestRunner : public ::testing::Test
{
public:
    static constexpr size_t thread_num = 5;

    static void submitAndWait(std::vector<TaskPtr> & tasks, PipelineExecutorContext & exec_context)
    {
        DB::LocalAdmissionController::global_instance = std::make_unique<DB::MockLocalAdmissionController>();
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        std::chrono::seconds timeout(15);
        exec_context.waitFor(timeout);
    }
};

TEST_F(TaskSchedulerTestRunner, simpleTask)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        PipelineExecutorContext exec_context;
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleTask>(exec_context));
        submitAndWait(tasks, exec_context);
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, simpleWaitingTask)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        PipelineExecutorContext exec_context;
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleWaitingTask>(exec_context));
        submitAndWait(tasks, exec_context);
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, testMemoryTrace)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        PipelineExecutorContext exec_context{"", "", MemoryTracker::create()};
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<MemoryTraceTask>(exec_context));
        submitAndWait(tasks, exec_context);
        // The value of the memory tracer is not checked here because of `std::memory_order_relaxed`.
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, shutdown)
try
{
    auto do_test = [&](size_t task_thread_pool_size, size_t task_num) {
        PipelineExecutorContext exec_context;
        {
            DB::LocalAdmissionController::global_instance = std::make_unique<DB::MockLocalAdmissionController>();
            TaskSchedulerConfig config{task_thread_pool_size, task_thread_pool_size};
            TaskScheduler task_scheduler{config};
            std::vector<TaskPtr> tasks;
            for (size_t i = 0; i < task_num; ++i)
                tasks.push_back(std::make_unique<DeadLoopTask>(exec_context));
            task_scheduler.submit(tasks);
        }
        std::chrono::seconds timeout(15);
        exec_context.waitFor(timeout);
    };
    std::vector<size_t> thread_nums{1, 5, 10, 100};
    for (auto task_thread_pool_size : thread_nums)
    {
        std::vector<size_t> task_nums{0, 1, 5, 10, 100, 200};
        for (auto task_num : task_nums)
            do_test(task_thread_pool_size, task_num);
    }
}
CATCH

} // namespace DB::tests
