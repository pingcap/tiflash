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
        : EventTask(nullptr, exec_status_, event_)
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
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<BaseTask>(exec_status, shared_from_this(), counter));
        scheduleTasks(tasks);
        return false;
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
        : EventTask(nullptr, exec_status_, event_)
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
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        if (!with_tasks)
            return true;

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<RunTask>(exec_status, shared_from_this()));
        scheduleTasks(tasks);
        return false;
    }

private:
    bool with_tasks;
};

class WaitCancelTask : public EventTask
{
public:
    explicit WaitCancelTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(nullptr, exec_status_, event_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return ExecTaskStatus::RUNNING;
    }
};

class WaitCancelEvent : public Event
{
public:
    WaitCancelEvent(
        PipelineExecutorStatus & exec_status_,
        bool with_tasks_)
        : Event(exec_status_, nullptr)
        , with_tasks(with_tasks_)
    {}

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        if (!with_tasks)
        {
            while (!exec_status.isCancelled())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return true;
        }

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<WaitCancelTask>(exec_status, shared_from_this()));
        scheduleTasks(tasks);
        return false;
    }

private:
    bool with_tasks;
};

class ToErrEvent : public Event
{
public:
    explicit ToErrEvent(PipelineExecutorStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {}

    static constexpr auto err_msg = "error from ToErrEvent";

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.onErrorOccurred(err_msg);
        return true;
    }
};

class AssertMemoryTraceEvent : public Event
{
public:
    AssertMemoryTraceEvent(PipelineExecutorStatus & exec_status_, MemoryTrackerPtr mem_tracker_)
        : Event(exec_status_, std::move(mem_tracker_))
    {}

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        assert(mem_tracker.get() == current_memory_tracker);
        return true;
    }

    void finishImpl() override
    {
        assert(mem_tracker.get() == current_memory_tracker);
    }
};

class CrashEvent : public Event
{
public:
    explicit CrashEvent(PipelineExecutorStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {}

protected:
    bool scheduleImpl() override
    {
        throw Exception("scheduleImpl");
    }

    void finishImpl() override
    {
        throw Exception("finishImpl");
    }
};

class ThrowExceptionTask : public EventTask
{
public:
    ThrowExceptionTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(nullptr, exec_status_, event_)
    {}

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        throw Exception("throw exception");
    }
};

class ThrowExceptionTaskEvent : public Event
{
public:
    explicit ThrowExceptionTaskEvent(PipelineExecutorStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {}

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<ThrowExceptionTask>(exec_status, shared_from_this()));
        scheduleTasks(tasks);
        return false;
    }
};
} // namespace

class EventTestRunner : public ::testing::Test
{
public:
    void schedule(std::vector<EventPtr> & events, std::shared_ptr<ThreadManager> thread_manager = nullptr)
    {
        Events without_input_events;
        for (const auto & event : events)
        {
            if (event->withoutInput())
                without_input_events.push_back(event);
        }
        for (const auto & event : without_input_events)
        {
            if (thread_manager)
                thread_manager->schedule(false, "event", [event]() { event->schedule(); });
            else
                event->schedule();
        }
    }

    void wait(PipelineExecutorStatus & exec_status)
    {
        std::chrono::seconds timeout(15);
        exec_status.waitFor(timeout);
    }

    void assertNoErr(PipelineExecutorStatus & exec_status)
    {
        auto err_msg = exec_status.getErrMsg();
        ASSERT_TRUE(err_msg.empty()) << err_msg;
    }

protected:
    static constexpr size_t thread_num = 5;

    void SetUp() override
    {
        TaskSchedulerConfig config{thread_num};
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
        ASSERT_TRUE(0 == counter);
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
                auto wait_cancel_event = std::make_shared<WaitCancelEvent>(exec_status, with_tasks);
                events.push_back(wait_cancel_event);
                // Expected to_err_event will not be triggered.
                auto to_err_event = std::make_shared<ToErrEvent>(exec_status);
                to_err_event->addInput(wait_cancel_event);
                events.push_back(to_err_event);
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
    auto do_test = [&](bool with_tasks, size_t wait_cancel_event_num) {
        PipelineExecutorStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < wait_cancel_event_num; ++i)
                events.push_back(std::make_shared<WaitCancelEvent>(exec_status, with_tasks));
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        {
            auto to_err_event = std::make_shared<ToErrEvent>(exec_status);
            assert(to_err_event->withoutInput());
            to_err_event->schedule();
        }
        wait(exec_status);
        auto err_msg = exec_status.getErrMsg();
        ASSERT_EQ(err_msg, ToErrEvent::err_msg) << err_msg;
        thread_manager->wait();
    };
    for (size_t i = 1; i < 100; i += 7)
    {
        do_test(false, i);
        do_test(true, i);
    }
}
CATCH

TEST_F(EventTestRunner, memory_trace)
try
{
    PipelineExecutorStatus exec_status;
    auto tracker = MemoryTracker::create();
    auto event = std::make_shared<AssertMemoryTraceEvent>(exec_status, tracker);
    event->schedule();
    wait(exec_status);
    assertNoErr(exec_status);
}
CATCH

TEST_F(EventTestRunner, crash)
try
{
    PipelineExecutorStatus exec_status;
    std::vector<EventPtr> events;
    // crash_event <-- run_event should run first,
    // otherwise the thread pool will be filled up by WaitCancelEvent/WaitCancelTask,
    // resulting in a period of time before RunEvent/RunTask/CrashEvent will run.
    auto run_event = std::make_shared<RunEvent>(exec_status, /*with_tasks=*/true);
    events.push_back(run_event);
    auto crash_event = std::make_shared<CrashEvent>(exec_status);
    crash_event->addInput(run_event);
    events.push_back(crash_event);

    for (size_t i = 0; i < 100; ++i)
        events.push_back(std::make_shared<WaitCancelEvent>(exec_status, /*with_tasks=*/true));

    schedule(events);
    wait(exec_status);
    auto err_msg = exec_status.getErrMsg();
    ASSERT_TRUE(!err_msg.empty());
}
CATCH

TEST_F(EventTestRunner, throw_exception_task)
try
{
    PipelineExecutorStatus exec_status;
    std::vector<EventPtr> events;
    // crash_event <-- run_event should run first,
    // otherwise the thread pool will be filled up by WaitCancelEvent/WaitCancelTask,
    // resulting in a period of time before RunEvent/RunTask/CrashEvent will run.
    auto run_event = std::make_shared<RunEvent>(exec_status, /*with_tasks=*/true);
    events.push_back(run_event);
    auto crash_event = std::make_shared<ThrowExceptionTaskEvent>(exec_status);
    crash_event->addInput(run_event);
    events.push_back(crash_event);

    for (size_t i = 0; i < 100; ++i)
        events.push_back(std::make_shared<WaitCancelEvent>(exec_status, /*with_tasks=*/true));

    schedule(events);
    wait(exec_status);
    auto err_msg = exec_status.getErrMsg();
    ASSERT_TRUE(!err_msg.empty());
}
CATCH

} // namespace DB::tests
