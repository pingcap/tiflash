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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
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
        : waiter(waiter_)
    {}

    ~SimpleTask()
    {
        waiter.notify();
    }

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
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
        : waiter(waiter_)
    {}

    ~SimpleWaitingTask()
    {
        waiter.notify();
    }

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
    Waiter & waiter;
};

class SimpleBlockedTask : public Task
{
public:
    explicit SimpleBlockedTask(Waiter & waiter_)
        : waiter(waiter_)
    {}

    ~SimpleBlockedTask()
    {
        waiter.notify();
    }

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
                return ExecTaskStatus::IO;
            else
            {
                --loop_count;
                return ExecTaskStatus::RUNNING;
            }
        }
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus executeIOImpl() noexcept override
    {
        if (loop_count > 0)
        {
            if ((loop_count % 2) == 0)
            {
                --loop_count;
                return ExecTaskStatus::IO;
            }
            else
                return ExecTaskStatus::RUNNING;
        }
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
    io,
    waiting,
};
class MemoryTraceTask : public Task
{
public:
    MemoryTraceTask(MemoryTrackerPtr mem_tracker_, Waiter & waiter_)
        : Task(std::move(mem_tracker_), "")
        , waiter(waiter_)
    {}

    ~MemoryTraceTask()
    {
        waiter.notify();
    }

    // From CurrentMemoryTracker::MEMORY_TRACER_SUBMIT_THRESHOLD
    static constexpr Int64 MEMORY_TRACER_SUBMIT_THRESHOLD = 1024 * 1024; // 1 MiB

protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        switch (status)
        {
        case TraceTaskStatus::initing:
            status = TraceTaskStatus::io;
            return ExecTaskStatus::IO;
        case TraceTaskStatus::io:
            status = TraceTaskStatus::waiting;
            return ExecTaskStatus::WAITING;
        case TraceTaskStatus::waiting:
        {
            status = TraceTaskStatus::running;
            CurrentMemoryTracker::alloc(MEMORY_TRACER_SUBMIT_THRESHOLD);
            return ExecTaskStatus::FINISHED;
        }
        default:
            __builtin_unreachable();
        }
    }

    ExecTaskStatus executeIOImpl() noexcept override
    {
        assert(status == TraceTaskStatus::io);
        CurrentMemoryTracker::alloc(MEMORY_TRACER_SUBMIT_THRESHOLD + 10);
        return ExecTaskStatus::RUNNING;
    }

    ExecTaskStatus awaitImpl() noexcept override
    {
        if (status == TraceTaskStatus::waiting)
            CurrentMemoryTracker::alloc(MEMORY_TRACER_SUBMIT_THRESHOLD - 10);
        return ExecTaskStatus::RUNNING;
    }

private:
    TraceTaskStatus status{TraceTaskStatus::initing};
    Waiter & waiter;
};

class DeadLoopTask : public Task
{
protected:
    ExecTaskStatus executeImpl() noexcept override
    {
        return ExecTaskStatus::WAITING;
    }

    ExecTaskStatus awaitImpl() noexcept override
    {
        return ExecTaskStatus::IO;
    }

    ExecTaskStatus executeIOImpl() noexcept override
    {
        return ExecTaskStatus::RUNNING;
    }
};
} // namespace

class TaskSchedulerTestRunner : public ::testing::Test
{
public:
    static constexpr size_t thread_num = 5;

    void submitAndWait(std::vector<TaskPtr> & tasks, Waiter & waiter)
    {
        TaskSchedulerConfig config{thread_num, thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        waiter.wait();
    }
};

TEST_F(TaskSchedulerTestRunner, simple_task)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleTask>(waiter));
        submitAndWait(tasks, waiter);
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
        submitAndWait(tasks, waiter);
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, test_memory_trace)
try
{
    for (size_t task_num = 1; task_num < 100; ++task_num)
    {
        auto tracker = MemoryTracker::create();
        MemoryTrackerSetter memory_tracker_setter{true, tracker.get()};
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<MemoryTraceTask>(tracker, waiter));
        submitAndWait(tasks, waiter);
        // The value of the memory tracer is not checked here because of `std::memory_order_relaxed`.
    }
}
CATCH

TEST_F(TaskSchedulerTestRunner, shutdown)
try
{
    auto do_test = [](size_t task_thread_pool_size, size_t task_num) {
        TaskSchedulerConfig config{task_thread_pool_size, task_thread_pool_size};
        TaskScheduler task_scheduler{config};
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<DeadLoopTask>());
        task_scheduler.submit(tasks);
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
