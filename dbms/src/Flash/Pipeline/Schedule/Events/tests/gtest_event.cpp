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

#include <Common/ThreadManager.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class BaseTask : public EventTask
{
public:
    BaseTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_,
        std::atomic_int64_t & counter_)
        : EventTask(exec_status_, event_)
        , counter(counter_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        --counter;
        return ExecTaskStatus::FINISHED;
    }

private:
    std::atomic_int64_t & counter;
};

class BaseEvent : public Event
{
public:
    BaseEvent(
        PipelineExecutorStatus & exec_status_,
        std::atomic_int64_t & counter_)
        : Event(exec_status_, nullptr)
        , counter(counter_)
    {}

    static constexpr auto task_num = 10;

protected:
    void scheduleImpl() override
    {
        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<BaseTask>(exec_status, shared_from_this(), counter));
    }

    void finishImpl() override
    {
        --counter;
    }

private:
    std::atomic_int64_t & counter;
};

class RunTask : public EventTask
{
public:
    RunTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(exec_status_, event_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
        return ExecTaskStatus::FINISHED;
    }

private:
    int loop_count = 5;
};

class RunEvent : public Event
{
public:
    RunEvent(
        PipelineExecutorStatus & exec_status_,
        bool with_tasks_)
        : Event(exec_status_, nullptr)
        , with_tasks(with_tasks_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_tasks)
            return;

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<RunTask>(exec_status, shared_from_this()));
    }

private:
    bool with_tasks;
};

class DeadLoopTask : public EventTask
{
public:
    DeadLoopTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(exec_status_, event_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return ExecTaskStatus::RUNNING;
    }
};

class DeadLoopEvent : public Event
{
public:
    DeadLoopEvent(
        PipelineExecutorStatus & exec_status_,
        bool with_tasks_)
        : Event(exec_status_, nullptr)
        , with_tasks(with_tasks_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_tasks)
        {
            while (!exec_status.isCancelled())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return;
        }

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<DeadLoopTask>(exec_status, shared_from_this()));
    }

private:
    bool with_tasks;
};

class OnErrEvent : public Event
{
public:
    explicit OnErrEvent(PipelineExecutorStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {}

    static constexpr auto err_msg = "error from OnErrEvent";

protected:
    void scheduleImpl() override
    {
        exec_status.onErrorOccurred(err_msg);
    }
};

class AssertMemoryTraceEvent : public Event
{
public:
    AssertMemoryTraceEvent(PipelineExecutorStatus & exec_status_, MemoryTrackerPtr mem_tracker_)
        : Event(exec_status_, std::move(mem_tracker_))
    {}

protected:
    void scheduleImpl() override
    {
        assert(mem_tracker.get() == current_memory_tracker);
    }

    void finishImpl() override
    {
        assert(mem_tracker.get() == current_memory_tracker);
    }
};

class ThrowExceptionTask : public EventTask
{
public:
    ThrowExceptionTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(exec_status_, event_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        throw Exception("throw exception in doExecuteImpl");
    }
};

class ThrowExceptionEvent : public Event
{
public:
    ThrowExceptionEvent(
        PipelineExecutorStatus & exec_status_,
        bool with_task_)
        : Event(exec_status_, nullptr)
        , with_task(with_task_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_task)
            throw Exception("throw exception in scheduleImpl");

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<ThrowExceptionTask>(exec_status, shared_from_this()));
    }

    void finishImpl() override
    {
        if (!with_task)
            throw Exception("throw exception in finishImpl");
    }

private:
    bool with_task;
};

class ManyTasksEvent : public Event
{
public:
    ManyTasksEvent(
        PipelineExecutorStatus & exec_status_,
        size_t task_num_)
        : Event(exec_status_, nullptr)
        , task_num(task_num_)
    {}

protected:
    void scheduleImpl() override
    {
        if (0 == task_num)
            return;

        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<RunTask>(exec_status, shared_from_this()));
    }

private:
    size_t task_num;
};

class DoInsertEvent : public Event
{
public:
    DoInsertEvent(
        PipelineExecutorStatus & exec_status_,
        int16_t & counter_)
        : Event(exec_status_, nullptr)
        , counter(counter_)
    {
        assert(counter > 0);
    }

protected:
    void scheduleImpl() override
    {
        addTask(std::make_unique<RunTask>(exec_status, shared_from_this()));
    }

    void finishImpl() override
    {
        --counter;
        if (counter > 0)
            insertEvent(std::make_shared<DoInsertEvent>(exec_status, counter));
    }

private:
    int16_t & counter;
};

class CreateTaskFailEvent : public Event
{
public:
    explicit CreateTaskFailEvent(PipelineExecutorStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {
    }

protected:
    void scheduleImpl() override
    {
        addTask(std::make_unique<RunTask>(exec_status, shared_from_this()));
        addTask(std::make_unique<RunTask>(exec_status, shared_from_this()));
        throw Exception("create task fail");
    }
};

class TestPorfileTask : public EventTask
{
public:
    static constexpr size_t min_time = 500'000'000L; // 500ms

    static constexpr size_t per_execute_time = 10'000'000L; // 10ms

    TestPorfileTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(exec_status_, event_)
    {}

protected:
    // doExecuteImpl min_time ==> doExecuteIOImpl min_time ==> doAwaitImpl min_time.
    ExecTaskStatus doExecuteImpl() override
    {
        if (cpu_execute_time < min_time)
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(per_execute_time));
            cpu_execute_time += per_execute_time;
            return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::IO_IN;
    }

    ExecTaskStatus doExecuteIOImpl() override
    {
        if (io_execute_time < min_time)
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(per_execute_time));
            io_execute_time += per_execute_time;
            return ExecTaskStatus::IO_IN;
        }
        return ExecTaskStatus::WAITING;
    }

    ExecTaskStatus doAwaitImpl() override
    {
        if unlikely (!wait_stopwatch)
            wait_stopwatch.emplace(CLOCK_MONOTONIC_COARSE);
        return wait_stopwatch->elapsed() < min_time
            ? ExecTaskStatus::WAITING
            : ExecTaskStatus::FINISHED;
    }

private:
    size_t cpu_execute_time = 0;
    size_t io_execute_time = 0;
    std::optional<Stopwatch> wait_stopwatch;
};

class TestPorfileEvent : public Event
{
public:
    explicit TestPorfileEvent(PipelineExecutorStatus & exec_status_, size_t task_num_)
        : Event(exec_status_, nullptr)
        , task_num(task_num_)
    {}

    const size_t task_num;

protected:
    void scheduleImpl() override
    {
        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<TestPorfileTask>(exec_status, shared_from_this()));
    }
};
} // namespace

class EventTestRunner : public ::testing::Test
{
public:
    static void schedule(std::vector<EventPtr> & events, std::shared_ptr<ThreadManager> thread_manager = nullptr)
    {
        Events sources;
        for (const auto & event : events)
        {
            if (event->prepare())
                sources.push_back(event);
        }
        for (const auto & event : sources)
        {
            if (thread_manager)
                thread_manager->schedule(false, "event", [event]() { event->schedule(); });
            else
                event->schedule();
        }
    }

    static void wait(PipelineExecutorStatus & exec_status)
    {
        std::chrono::seconds timeout(15);
        exec_status.waitFor(timeout);
    }

    static void assertNoErr(PipelineExecutorStatus & exec_status)
    {
        auto exception_ptr = exec_status.getExceptionPtr();
        auto exception_msg = exec_status.getExceptionMsg();
        ASSERT_TRUE(!exception_ptr) << exception_msg;
    }

protected:
    static constexpr size_t thread_num = 5;

    void SetUp() override
    {
        TaskSchedulerConfig config{thread_num, thread_num};
        assert(!TaskScheduler::instance);
        TaskScheduler::instance = std::make_unique<TaskScheduler>(config);
    }

    void TearDown() override
    {
        assert(TaskScheduler::instance);
        TaskScheduler::instance.reset();
    }
};

TEST_F(EventTestRunner, base)
try
{
    auto do_test = [&](size_t group_num, size_t event_num) {
        // group_num * (event_num * (`BaseEvent::finishImpl + BaseEvent::task_num * ~BaseTask()`))
        std::atomic_int64_t counter{static_cast<int64_t>(group_num * (event_num * (1 + BaseEvent::task_num)))};
        PipelineExecutorStatus exec_status;
        {
            std::vector<EventPtr> all_events;
            for (size_t i = 0; i < group_num; ++i)
            {
                std::vector<EventPtr> events;
                EventPtr start;
                for (size_t j = 0; j < event_num; ++j)
                {
                    auto event = std::make_shared<BaseEvent>(exec_status, counter);
                    if (!events.empty())
                        event->addInput(events.back());
                    events.push_back(event);
                }
                all_events.insert(all_events.end(), events.begin(), events.end());
            }
            schedule(all_events);
        }
        wait(exec_status);
        ASSERT_EQ(0, counter);
        assertNoErr(exec_status);
    };
    for (size_t group_num = 1; group_num < 50; group_num += 11)
    {
        for (size_t event_num = 1; event_num < 50; event_num += 11)
            do_test(group_num, event_num);
    }
}
CATCH

TEST_F(EventTestRunner, run)
try
{
    auto do_test = [&](bool with_tasks, size_t event_num) {
        PipelineExecutorStatus exec_status;
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_num; ++i)
                events.push_back(std::make_shared<RunEvent>(exec_status, with_tasks));
            schedule(events);
        }
        wait(exec_status);
        assertNoErr(exec_status);
    };
    for (size_t i = 1; i < 100; i += 7)
    {
        do_test(false, i);
        do_test(true, i);
    }
}
CATCH

TEST_F(EventTestRunner, cancel)
try
{
    auto do_test = [&](bool with_tasks, size_t event_batch_num) {
        PipelineExecutorStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_batch_num; ++i)
            {
                auto dead_loop_event = std::make_shared<DeadLoopEvent>(exec_status, with_tasks);
                events.push_back(dead_loop_event);
                // Expected on_err_event will not be triggered.
                auto on_err_event = std::make_shared<OnErrEvent>(exec_status);
                on_err_event->addInput(dead_loop_event);
                events.push_back(on_err_event);
            }
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        exec_status.cancel();
        wait(exec_status);
        assertNoErr(exec_status);
        thread_manager->wait();
    };
    for (size_t i = 1; i < 100; i += 7)
    {
        do_test(false, i);
        do_test(true, i);
    }
}
CATCH

TEST_F(EventTestRunner, err)
try
{
    auto do_test = [&](bool with_tasks, size_t dead_loop_event_num) {
        PipelineExecutorStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < dead_loop_event_num; ++i)
                events.push_back(std::make_shared<DeadLoopEvent>(exec_status, with_tasks));
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        {
            auto on_err_event = std::make_shared<OnErrEvent>(exec_status);
            if (on_err_event->prepare())
                on_err_event->schedule();
        }
        wait(exec_status);
        auto err_msg = exec_status.getExceptionMsg();
        ASSERT_EQ(err_msg, OnErrEvent::err_msg) << err_msg;
        thread_manager->wait();
    };
    for (size_t i = 1; i < 100; i += 7)
    {
        do_test(false, i);
        do_test(true, i);
    }
}
CATCH

TEST_F(EventTestRunner, memoryTrace)
try
{
    PipelineExecutorStatus exec_status;
    auto tracker = MemoryTracker::create();
    auto event = std::make_shared<AssertMemoryTraceEvent>(exec_status, tracker);
    if (event->prepare())
        event->schedule();
    wait(exec_status);
    assertNoErr(exec_status);
}
CATCH

TEST_F(EventTestRunner, throwException)
try
{
    std::vector<bool> with_tasks{false, true};
    for (auto with_task : with_tasks)
    {
        PipelineExecutorStatus exec_status;
        std::vector<EventPtr> events;
        // throw_exception_event <-- run_event should run first,
        // otherwise the thread pool will be filled up by DeadLoopEvent/DeadLoopTask,
        // resulting in a period of time before RunEvent/RunTask/ThrowExceptionEvent will run.
        auto run_event = std::make_shared<RunEvent>(exec_status, /*with_tasks=*/true);
        events.push_back(run_event);
        auto crash_event = std::make_shared<ThrowExceptionEvent>(exec_status, with_task);
        crash_event->addInput(run_event);
        events.push_back(crash_event);

        for (size_t i = 0; i < 100; ++i)
            events.push_back(std::make_shared<DeadLoopEvent>(exec_status, /*with_tasks=*/true));

        schedule(events);
        wait(exec_status);
        auto exception_ptr = exec_status.getExceptionPtr();
        ASSERT_TRUE(exception_ptr);
    }
}
CATCH

TEST_F(EventTestRunner, manyTasks)
try
{
    for (size_t i = 0; i < 200; i += 7)
    {
        PipelineExecutorStatus exec_status;
        auto event = std::make_shared<ManyTasksEvent>(exec_status, i);
        if (event->prepare())
            event->schedule();
        wait(exec_status);
        assertNoErr(exec_status);
    }
}
CATCH

TEST_F(EventTestRunner, insertEvents)
try
{
    PipelineExecutorStatus exec_status;
    std::vector<int16_t> counters;
    for (size_t i = 0; i < 10; ++i)
        counters.push_back(99);
    {
        std::vector<EventPtr> events;
        events.reserve(counters.size());
        for (auto & counter : counters)
            events.push_back(std::make_shared<DoInsertEvent>(exec_status, counter));
        auto err_event = std::make_shared<ThrowExceptionEvent>(exec_status, false);
        for (const auto & event : events)
            err_event->addInput(event);
        events.push_back(err_event);
        schedule(events);
    }
    wait(exec_status);
    auto exception_ptr = exec_status.getExceptionPtr();
    ASSERT_TRUE(exception_ptr);
    for (auto & counter : counters)
        ASSERT_EQ(0, counter);
}
CATCH

TEST_F(EventTestRunner, createTaskFail)
try
{
    PipelineExecutorStatus exec_status;
    auto event = std::make_shared<CreateTaskFailEvent>(exec_status);
    if (event->prepare())
        event->schedule();
    wait(exec_status);
    auto exception_ptr = exec_status.getExceptionPtr();
    ASSERT_TRUE(exception_ptr);
}
CATCH

TEST_F(EventTestRunner, profile)
try
{
    for (size_t task_num = 0; task_num < 2 * thread_num; task_num += 2)
    {
        PipelineExecutorStatus exec_status;
        auto event = std::make_shared<TestPorfileEvent>(exec_status, task_num);
        if (event->prepare())
            event->schedule();
        wait(exec_status);
        assertNoErr(exec_status);

        /// for executing
        size_t exec_lower_limit = task_num * TestPorfileTask::min_time;
        // Use `exec_lower_limit * 5` to avoid failure caused by unstable test environment.
        size_t exec_upper_limit = exec_lower_limit * 5;
        auto do_assert_for_exec = [&](UInt64 value) {
            ASSERT_GE(value, exec_lower_limit);
            ASSERT_LE(value, exec_upper_limit);
        };
        do_assert_for_exec(exec_status.getQueryProfileInfo().getCPUExecuteTimeNs());
        do_assert_for_exec(exec_status.getQueryProfileInfo().getIOExecuteTimeNs());
        do_assert_for_exec(exec_status.getQueryProfileInfo().getAwaitTimeNs());

        /// for pending
        if (task_num > thread_num)
        {
            // If the number of tasks is greater than the number of threads, there must be tasks in a pending state.
            // To avoid unstable unit tests, we do not check the upper limit.
            ASSERT_GT(exec_status.getQueryProfileInfo().getCPUPendingTimeNs(), 0);
            ASSERT_GT(exec_status.getQueryProfileInfo().getIOPendingTimeNs(), 0);
        }
    }
}
CATCH

} // namespace DB::tests
