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

#include <Common/ThreadManager.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class BaseTask : public EventTask
{
public:
    BaseTask(PipelineExecutorContext & exec_context_, const EventPtr & event_, std::atomic_int64_t & counter_)
        : EventTask(exec_context_, event_)
        , counter(counter_)
    {}

protected:
    ExecTaskStatus executeImpl() override
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
    BaseEvent(PipelineExecutorContext & exec_context_, std::atomic_int64_t & counter_)
        : Event(exec_context_)
        , counter(counter_)
    {}

    static constexpr auto task_num = 10;

protected:
    void scheduleImpl() override
    {
        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<BaseTask>(exec_context, shared_from_this(), counter));
    }

    void finishImpl() override { --counter; }

private:
    std::atomic_int64_t & counter;
};

class RunTask : public EventTask
{
public:
    RunTask(PipelineExecutorContext & exec_context_, const EventPtr & event_)
        : EventTask(exec_context_, event_)
    {}

protected:
    ExecTaskStatus executeImpl() override
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
    RunEvent(PipelineExecutorContext & exec_context_, bool with_tasks_)
        : Event(exec_context_)
        , with_tasks(with_tasks_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_tasks)
            return;

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<RunTask>(exec_context, shared_from_this()));
    }

private:
    bool with_tasks;
};

class DeadLoopTask : public EventTask
{
public:
    DeadLoopTask(PipelineExecutorContext & exec_context_, const EventPtr & event_)
        : EventTask(exec_context_, event_)
    {}

protected:
    ExecTaskStatus executeImpl() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return ExecTaskStatus::RUNNING;
    }
};

class DeadLoopEvent : public Event
{
public:
    DeadLoopEvent(PipelineExecutorContext & exec_context_, bool with_tasks_)
        : Event(exec_context_)
        , with_tasks(with_tasks_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_tasks)
        {
            while (!exec_context.isCancelled())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return;
        }

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<DeadLoopTask>(exec_context, shared_from_this()));
    }

private:
    bool with_tasks;
};

class OnErrEvent : public Event
{
public:
    explicit OnErrEvent(PipelineExecutorContext & exec_context_)
        : Event(exec_context_)
    {}

    static constexpr auto err_msg = "error from OnErrEvent";

protected:
    void scheduleImpl() override { exec_context.onErrorOccurred(err_msg); }
};

class AssertMemoryTraceEvent : public Event
{
public:
    explicit AssertMemoryTraceEvent(PipelineExecutorContext & exec_context_)
        : Event(exec_context_)
    {
        assert(mem_tracker != nullptr);
    }

protected:
    void scheduleImpl() override { assert(mem_tracker.get() == current_memory_tracker); }

    void finishImpl() override { assert(mem_tracker.get() == current_memory_tracker); }
};

class ThrowExceptionTask : public EventTask
{
public:
    ThrowExceptionTask(PipelineExecutorContext & exec_context_, const EventPtr & event_)
        : EventTask(exec_context_, event_)
    {}

protected:
    ExecTaskStatus executeImpl() override { throw Exception("throw exception in doExecuteImpl"); }
};

class ThrowExceptionEvent : public Event
{
public:
    ThrowExceptionEvent(PipelineExecutorContext & exec_context_, bool with_task_)
        : Event(exec_context_)
        , with_task(with_task_)
    {}

protected:
    void scheduleImpl() override
    {
        if (!with_task)
            throw Exception("throw exception in scheduleImpl");

        for (size_t i = 0; i < 10; ++i)
            addTask(std::make_unique<ThrowExceptionTask>(exec_context, shared_from_this()));
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
    ManyTasksEvent(PipelineExecutorContext & exec_context_, size_t task_num_)
        : Event(exec_context_)
        , task_num(task_num_)
    {}

protected:
    void scheduleImpl() override
    {
        if (0 == task_num)
            return;

        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<RunTask>(exec_context, shared_from_this()));
    }

private:
    size_t task_num;
};

class DoInsertEvent : public Event
{
public:
    DoInsertEvent(PipelineExecutorContext & exec_context_, int16_t & counter_)
        : Event(exec_context_)
        , counter(counter_)
    {
        assert(counter > 0);
    }

protected:
    void scheduleImpl() override { addTask(std::make_unique<RunTask>(exec_context, shared_from_this())); }

    void finishImpl() override
    {
        --counter;
        if (counter > 0)
            insertEvent(std::make_shared<DoInsertEvent>(exec_context, counter));
    }

private:
    int16_t & counter;
};

class CreateTaskFailEvent : public Event
{
public:
    explicit CreateTaskFailEvent(PipelineExecutorContext & exec_context_)
        : Event(exec_context_)
    {}

protected:
    void scheduleImpl() override
    {
        addTask(std::make_unique<RunTask>(exec_context, shared_from_this()));
        addTask(std::make_unique<RunTask>(exec_context, shared_from_this()));
        throw Exception("create task fail");
    }
};

class TestPorfileTask : public EventTask
{
public:
    static constexpr size_t min_time = 500'000'000L; // 500ms

    static constexpr size_t per_execute_time = 10'000'000L; // 10ms

    TestPorfileTask(PipelineExecutorContext & exec_context_, const EventPtr & event_)
        : EventTask(exec_context_, event_)
    {}

protected:
    // executeImpl min_time ==> executeIOImpl min_time ==> awaitImpl min_time.
    ExecTaskStatus executeImpl() override
    {
        if (cpu_execute_time < min_time)
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(per_execute_time));
            cpu_execute_time += per_execute_time;
            return ExecTaskStatus::RUNNING;
        }
        return ExecTaskStatus::IO_IN;
    }

    ExecTaskStatus executeIOImpl() override
    {
        if (io_execute_time < min_time)
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(per_execute_time));
            io_execute_time += per_execute_time;
            return ExecTaskStatus::IO_IN;
        }
        return ExecTaskStatus::WAITING;
    }

    ExecTaskStatus awaitImpl() override
    {
        if unlikely (!wait_stopwatch)
            wait_stopwatch.emplace(CLOCK_MONOTONIC_COARSE);
        return wait_stopwatch->elapsed() < min_time ? ExecTaskStatus::WAITING : ExecTaskStatus::FINISHED;
    }

private:
    size_t cpu_execute_time = 0;
    size_t io_execute_time = 0;
    std::optional<Stopwatch> wait_stopwatch;
};

class TestPorfileEvent : public Event
{
public:
    explicit TestPorfileEvent(PipelineExecutorContext & exec_context_, size_t task_num_)
        : Event(exec_context_)
        , task_num(task_num_)
    {}

    const size_t task_num;

protected:
    void scheduleImpl() override
    {
        for (size_t i = 0; i < task_num; ++i)
            addTask(std::make_unique<TestPorfileTask>(exec_context, shared_from_this()));
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

    static void wait(PipelineExecutorContext & exec_context)
    {
        std::chrono::seconds timeout(15);
        exec_context.waitFor(timeout);
    }

    static void assertNoErr(PipelineExecutorContext & exec_context)
    {
        auto exception_ptr = exec_context.getExceptionPtr();
        auto exception_msg = exec_context.getExceptionMsg();
        ASSERT_TRUE(!exception_ptr) << exception_msg;
    }

protected:
    static constexpr size_t thread_num = 5;

    void SetUp() override
    {
        DB::LocalAdmissionController::global_instance = std::make_unique<DB::MockLocalAdmissionController>();
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
        PipelineExecutorContext exec_context;
        {
            std::vector<EventPtr> all_events;
            for (size_t i = 0; i < group_num; ++i)
            {
                std::vector<EventPtr> events;
                EventPtr start;
                for (size_t j = 0; j < event_num; ++j)
                {
                    auto event = std::make_shared<BaseEvent>(exec_context, counter);
                    if (!events.empty())
                        event->addInput(events.back());
                    events.push_back(event);
                }
                all_events.insert(all_events.end(), events.begin(), events.end());
            }
            schedule(all_events);
        }
        wait(exec_context);
        ASSERT_EQ(0, counter);
        assertNoErr(exec_context);
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
        PipelineExecutorContext exec_context;
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_num; ++i)
                events.push_back(std::make_shared<RunEvent>(exec_context, with_tasks));
            schedule(events);
        }
        wait(exec_context);
        assertNoErr(exec_context);
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
        PipelineExecutorContext exec_context;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_batch_num; ++i)
            {
                auto dead_loop_event = std::make_shared<DeadLoopEvent>(exec_context, with_tasks);
                events.push_back(dead_loop_event);
                // Expected on_err_event will not be triggered.
                auto on_err_event = std::make_shared<OnErrEvent>(exec_context);
                on_err_event->addInput(dead_loop_event);
                events.push_back(on_err_event);
            }
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        exec_context.cancel();
        wait(exec_context);
        assertNoErr(exec_context);
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
        PipelineExecutorContext exec_context;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < dead_loop_event_num; ++i)
                events.push_back(std::make_shared<DeadLoopEvent>(exec_context, with_tasks));
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        {
            auto on_err_event = std::make_shared<OnErrEvent>(exec_context);
            if (on_err_event->prepare())
                on_err_event->schedule();
        }
        wait(exec_context);
        auto err_msg = exec_context.getExceptionMsg();
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
    PipelineExecutorContext exec_context{"", "", MemoryTracker::create()};
    auto event = std::make_shared<AssertMemoryTraceEvent>(exec_context);
    if (event->prepare())
        event->schedule();
    wait(exec_context);
    assertNoErr(exec_context);
}
CATCH

TEST_F(EventTestRunner, throwException)
try
{
    std::vector<bool> with_tasks{false, true};
    for (auto with_task : with_tasks)
    {
        PipelineExecutorContext exec_context;
        std::vector<EventPtr> events;
        // throw_exception_event <-- run_event should run first,
        // otherwise the thread pool will be filled up by DeadLoopEvent/DeadLoopTask,
        // resulting in a period of time before RunEvent/RunTask/ThrowExceptionEvent will run.
        auto run_event = std::make_shared<RunEvent>(exec_context, /*with_tasks=*/true);
        events.push_back(run_event);
        auto crash_event = std::make_shared<ThrowExceptionEvent>(exec_context, with_task);
        crash_event->addInput(run_event);
        events.push_back(crash_event);

        for (size_t i = 0; i < 100; ++i)
            events.push_back(std::make_shared<DeadLoopEvent>(exec_context, /*with_tasks=*/true));

        schedule(events);
        wait(exec_context);
        auto exception_ptr = exec_context.getExceptionPtr();
        ASSERT_TRUE(exception_ptr);
    }
}
CATCH

TEST_F(EventTestRunner, manyTasks)
try
{
    for (size_t i = 0; i < 200; i += 7)
    {
        PipelineExecutorContext exec_context;
        auto event = std::make_shared<ManyTasksEvent>(exec_context, i);
        if (event->prepare())
            event->schedule();
        wait(exec_context);
        assertNoErr(exec_context);
    }
}
CATCH

TEST_F(EventTestRunner, insertEvents)
try
{
    PipelineExecutorContext exec_context;
    std::vector<int16_t> counters;
    for (size_t i = 0; i < 10; ++i)
        counters.push_back(99);
    {
        std::vector<EventPtr> events;
        events.reserve(counters.size());
        for (auto & counter : counters)
            events.push_back(std::make_shared<DoInsertEvent>(exec_context, counter));
        auto err_event = std::make_shared<ThrowExceptionEvent>(exec_context, false);
        for (const auto & event : events)
            err_event->addInput(event);
        events.push_back(err_event);
        schedule(events);
    }
    wait(exec_context);
    auto exception_ptr = exec_context.getExceptionPtr();
    ASSERT_TRUE(exception_ptr);
    for (auto & counter : counters)
        ASSERT_EQ(0, counter);
}
CATCH

TEST_F(EventTestRunner, createTaskFail)
try
{
    PipelineExecutorContext exec_context;
    auto event = std::make_shared<CreateTaskFailEvent>(exec_context);
    if (event->prepare())
        event->schedule();
    wait(exec_context);
    auto exception_ptr = exec_context.getExceptionPtr();
    ASSERT_TRUE(exception_ptr);
}
CATCH

TEST_F(EventTestRunner, profile)
try
{
    for (size_t task_num = 0; task_num < 2 * thread_num; task_num += 2)
    {
        PipelineExecutorContext exec_context;
        auto event = std::make_shared<TestPorfileEvent>(exec_context, task_num);
        if (event->prepare())
            event->schedule();
        wait(exec_context);
        assertNoErr(exec_context);

        /// for executing
        size_t exec_lower_limit = task_num * TestPorfileTask::min_time;
        // Use `exec_lower_limit * 5` to avoid failure caused by unstable test environment.
        size_t exec_upper_limit = exec_lower_limit * 5;
        auto do_assert_for_exec = [&](UInt64 value) {
            ASSERT_GE(value, exec_lower_limit);
            ASSERT_LE(value, exec_upper_limit);
        };
        do_assert_for_exec(exec_context.getQueryProfileInfo().getCPUExecuteTimeNs());
        do_assert_for_exec(exec_context.getQueryProfileInfo().getIOExecuteTimeNs());
        do_assert_for_exec(exec_context.getQueryProfileInfo().getAwaitTimeNs());

        /// for pending
        if (task_num > thread_num)
        {
            // If the number of tasks is greater than the number of threads, there must be tasks in a pending state.
            // To avoid unstable unit tests, we do not check the upper limit.
            ASSERT_GT(exec_context.getQueryProfileInfo().getCPUPendingTimeNs(), 0);
            ASSERT_GT(exec_context.getQueryProfileInfo().getIOPendingTimeNs(), 0);
        }
    }
}
CATCH

} // namespace DB::tests
