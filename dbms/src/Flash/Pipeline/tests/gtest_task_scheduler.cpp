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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class Waiter
{
public:
    Waiter(size_t init_value)
        : counter(init_value)
    {}
    void notify()
    {
        bool last_notify = false;
        {
            std::lock_guard lock(mu);
            last_notify = (--counter) == 0;
        }
        if (last_notify)
            cv.notify_one();
    }
    void wait()
    {
        std::chrono::seconds timeout(15);
        std::unique_lock lock(mu);
        RUNTIME_CHECK(cv.wait_for(lock, timeout, [&] { return 0 == counter; }));
    }

private:
    mutable std::mutex mu;
    int64_t counter;
    std::condition_variable cv;
};

class SimpleTask : public Task
{
public:
    explicit SimpleTask(Waiter & waiter_)
        : Task(nullptr)
        , waiter(waiter_)
    {}

    ExecTaskStatus executeImpl() override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
        waiter.notify();
        return ExecTaskStatus::FINISHED;
    }

private:
    Waiter & waiter;
    int loop_count = 5;
};

class SimpleWaitingTask : public Task
{
public:
    explicit SimpleWaitingTask(Waiter & waiter_)
        : Task(nullptr)
        , waiter(waiter_)
    {}

    ExecTaskStatus executeImpl() override
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
        waiter.notify();
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus awaitImpl() override
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
        waiter.notify();
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 10 + random() % 10;
    Waiter & waiter;
};

class SimpleSpillingTask : public Task
{
public:
    explicit SimpleSpillingTask(Waiter & waiter_)
        : Task(nullptr)
        , waiter(waiter_)
    {}

    ExecTaskStatus executeImpl() override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
                return ExecTaskStatus::SPILLING;
            else
            {
                --loop_count;
                return ExecTaskStatus::RUNNING;
            }
        }
        waiter.notify();
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus spillImpl() override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
            {
                --loop_count;
                return ExecTaskStatus::SPILLING;
            }
            else
                return ExecTaskStatus::RUNNING;
        }
        waiter.notify();
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 10 + random() % 10;
    Waiter & waiter;
};

enum class TraceTaskStatus
{
    initing,
    running,
    waiting,
    spilling,
};
class MemoryTraceTask : public Task
{
public:
    MemoryTraceTask(MemoryTrackerPtr mem_tracker_, Waiter & waiter_)
        : Task(std::move(mem_tracker_))
        , waiter(waiter_)
    {}

    ExecTaskStatus executeImpl() override
    {
        switch (status)
        {
        case TraceTaskStatus::initing:
            status = TraceTaskStatus::waiting;
            return ExecTaskStatus::WAITING;
        case TraceTaskStatus::waiting:
            status = TraceTaskStatus::spilling;
            return ExecTaskStatus::SPILLING;
        case TraceTaskStatus::spilling:
        {
            status = TraceTaskStatus::running;
            CurrentMemoryTracker::alloc(10);
            waiter.notify();
            return ExecTaskStatus::FINISHED;
        }
        default:
            __builtin_unreachable();
        }
    }

    ExecTaskStatus spillImpl() override
    {
        assert(status == TraceTaskStatus::spilling);
        CurrentMemoryTracker::alloc(10);
        return ExecTaskStatus::RUNNING;
    }

    ExecTaskStatus awaitImpl() override
    {
        if (status == TraceTaskStatus::waiting)
            CurrentMemoryTracker::alloc(10);
        return ExecTaskStatus::RUNNING;
    }

private:
    TraceTaskStatus status{TraceTaskStatus::initing};
    Waiter & waiter;
};
} // namespace

class TaskSchedulerTestRunner : public ::testing::Test
{
public:
    static constexpr size_t thread_num = 5;
};

TEST_F(TaskSchedulerTestRunner, shutdown)
try
{
    std::vector<size_t> thread_nums{1, 5, 10, 100};
    for (auto task_executor_thread_num : thread_nums)
    {
        for (auto spill_executor_thread_num : thread_nums)
        {
            TaskSchedulerConfig config{task_executor_thread_num, spill_executor_thread_num};
            TaskScheduler task_scheduler{config};
        }
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, simple_task)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleTask>(waiter));
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        waiter.wait();
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, simple_waiting_task)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleWaitingTask>(waiter));
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        waiter.wait();
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, simple_spilling_task)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleSpillingTask>(waiter));
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        waiter.wait();
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, test_memory_trace)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        Waiter waiter(task_num);
        auto tracker = MemoryTracker::create();
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<MemoryTraceTask>(tracker, waiter));
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        MemoryTrackerSetter memory_tracker_setter{true, tracker.get()};
        task_scheduler.submit(tasks);
        waiter.wait();
        // The value of the memory tracer is not checked here because of `std::memory_order_relaxed`.
    }
}
CATCH

} // namespace DB::tests
