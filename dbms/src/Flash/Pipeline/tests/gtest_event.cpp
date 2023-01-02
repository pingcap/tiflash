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

#include <Common/ThreadManager.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class BaseTask : public Task
{
public:
    explicit BaseTask(const EventPtr & event_, std::atomic_int64_t & counter_)
        : Task(nullptr)
        , event(event_)
        , counter(counter_)
    {}

    ~BaseTask()
    {
        event->finishTask();
        event.reset();
    }

protected:
    ExecTaskStatus executeImpl() override
    {
        --counter;
        return ExecTaskStatus::FINISHED;
    }

private:
    EventPtr event;
    std::atomic_int64_t & counter;
};

class BaseEvent : public Event
{
public:
    BaseEvent(
        PipelineExecStatus & exec_status_,
        std::atomic_int64_t & counter_)
        : Event(exec_status_, nullptr)
        , counter(counter_)
    {}

    static constexpr auto task_num = 10;

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.addActivePipeline();
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<BaseTask>(shared_from_this(), counter));
        scheduleTask(tasks);
        return false;
    }

    // Returns true meaning next_events will be scheduled.
    bool finishImpl() override { return true; }

    void finalizeFinish() override
    {
        --counter;
        exec_status.completePipeline();
    }

private:
    std::atomic_int64_t & counter;
};

class RunTask : public Task
{
public:
    explicit RunTask(const EventPtr & event_)
        : Task(nullptr)
        , event(event_)
    {}

    ~RunTask()
    {
        event->finishTask();
        event.reset();
    }

protected:
    ExecTaskStatus executeImpl() override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
        return ExecTaskStatus::FINISHED;
    }

private:
    EventPtr event;
    int loop_count = 5;
};

class RunEvent : public Event
{
public:
    RunEvent(
        PipelineExecStatus & exec_status_,
        bool with_tasks_)
        : Event(exec_status_, nullptr)
        , with_tasks(with_tasks_)
    {}

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.addActivePipeline();
        if (!with_tasks)
            return true;

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<RunTask>(shared_from_this()));
        scheduleTask(tasks);
        return false;
    }

    // Returns true meaning next_events will be scheduled.
    bool finishImpl() override { return true; }

    void finalizeFinish() override
    {
        exec_status.completePipeline();
    }

private:
    bool with_tasks;
};

class WaitCancelTask : public Task
{
public:
    explicit WaitCancelTask(const EventPtr & event_)
        : Task(nullptr)
        , event(event_)
    {}

    ~WaitCancelTask()
    {
        event->finishTask();
        event.reset();
    }

protected:
    ExecTaskStatus executeImpl() override
    {
        while (!event->isCancelled())
            return ExecTaskStatus::RUNNING;
        return ExecTaskStatus::CANCELLED;
    }

private:
    EventPtr event;
};

class WaitCancelEvent : public Event
{
public:
    WaitCancelEvent(
        PipelineExecStatus & exec_status_,
        bool with_tasks_)
        : Event(exec_status_, nullptr)
        , with_tasks(with_tasks_)
    {}

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.addActivePipeline();
        if (!with_tasks)
        {
            while (!isCancelled())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return true;
        }

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<WaitCancelTask>(shared_from_this()));
        scheduleTask(tasks);
        return false;
    }

    // Returns true meaning next_events will be scheduled.
    bool finishImpl() override { return !isCancelled(); }

    void finalizeFinish() override
    {
        exec_status.completePipeline();
    }

private:
    bool with_tasks;
};

class ToErrEvent : public Event
{
public:
    explicit ToErrEvent(PipelineExecStatus & exec_status_)
        : Event(exec_status_, nullptr)
    {}

    static constexpr auto err_msg = "error from ToErrEvent";

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.addActivePipeline();
        exec_status.toError(err_msg);
        return true;
    }

    void finalizeFinish() override
    {
        exec_status.completePipeline();
    }
};
} // namespace

class EventTestRunner : public ::testing::Test
{
public:
    void schedule(std::vector<EventPtr> & events, std::shared_ptr<ThreadManager> thread_manager = nullptr)
    {
        Events non_dependent_events;
        for (const auto & event : events)
        {
            if (event->isNonDependent())
                non_dependent_events.push_back(event);
        }
        for (const auto & event : non_dependent_events)
        {
            if (thread_manager)
                thread_manager->schedule(false, "event", [event]() { event->schedule(); });
            else
                event->schedule();
        }
    }

    void wait(PipelineExecStatus & exec_status)
    {
        std::chrono::seconds timeout(15);
        exec_status.waitFor(timeout);
    }

    void assertNoErr(PipelineExecStatus & exec_status)
    {
        auto err_msg = exec_status.getErrMsg();
        ASSERT_TRUE(err_msg.empty()) << err_msg;
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
    for (size_t event_num = 1; event_num < 200; ++event_num)
    {
        // BaseEvent::finalizeFinish + BaseEvent::task_num * ~BaseTask()
        std::atomic_int64_t counter{static_cast<int64_t>(event_num * (1 + BaseEvent::task_num))};
        PipelineExecStatus exec_status;
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_num; ++i)
            {
                auto event = std::make_shared<BaseEvent>(exec_status, counter);
                if (!events.empty())
                    event->addDependency(events.back());
                events.push_back(event);
            }
            schedule(events);
        }
        wait(exec_status);
        ASSERT_TRUE(0 == counter);
        assertNoErr(exec_status);
    }
}
CATCH

TEST_F(EventTestRunner, run)
try
{
    auto do_test = [&](bool with_tasks, size_t event_num) {
        PipelineExecStatus exec_status;
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
        PipelineExecStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < event_batch_num; ++i)
            {
                auto wait_cancel_event = std::make_shared<WaitCancelEvent>(exec_status, with_tasks);
                events.push_back(wait_cancel_event);
                // Expected to_err_event will not be triggered.
                auto to_err_event = std::make_shared<ToErrEvent>(exec_status);
                to_err_event->addDependency(wait_cancel_event);
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
        PipelineExecStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            std::vector<EventPtr> events;
            for (size_t i = 0; i < wait_cancel_event_num; ++i)
                events.push_back(std::make_shared<WaitCancelEvent>(exec_status, with_tasks));
            schedule(events, with_tasks ? nullptr : thread_manager);
        }
        {
            auto to_err_event = std::make_shared<ToErrEvent>(exec_status);
            assert(to_err_event->isNonDependent());
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

} // namespace DB::tests
