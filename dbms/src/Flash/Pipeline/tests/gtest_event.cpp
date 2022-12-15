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
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class SimpleTask : public Task
{
public:
    explicit SimpleTask(const EventPtr & event_)
        : Task(nullptr)
        , event(event_)
    {}

    ExecTaskStatus executeImpl() override
    {
        while ((--loop_count) > 0)
            return ExecTaskStatus::RUNNING;
        event->finishTask();
        return ExecTaskStatus::FINISHED;
    }

private:
    EventPtr event;
    int loop_count = 5;
};

class SimpleEvent : public Event
{
public:
    SimpleEvent(
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
            tasks.push_back(std::make_unique<SimpleTask>(shared_from_this()));
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

class SimpleCancelTask : public Task
{
public:
    explicit SimpleCancelTask(const EventPtr & event_)
        : Task(nullptr)
        , event(event_)
    {}

    ExecTaskStatus executeImpl() override
    {
        while (!event->isCancelled())
            return ExecTaskStatus::RUNNING;
        event->finishTask();
        return ExecTaskStatus::CANCELLED;
    }

private:
    EventPtr event;
};

class SimpleCancelEvent : public Event
{
public:
    SimpleCancelEvent(
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
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            return true;
        }

        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < 10; ++i)
            tasks.push_back(std::make_unique<SimpleCancelTask>(shared_from_this()));
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

protected:
    // Returns true meaning no task is scheduled.
    bool scheduleImpl() override
    {
        exec_status.addActivePipeline();
        exec_status.toError("Shouldn't have arrived here");
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
    static constexpr size_t thread_num = 5;

protected:
    void SetUp() override
    {
        TaskSchedulerConfig config{thread_num, 0};
        TaskScheduler::instance = std::make_unique<TaskScheduler>(config);
    }

    void TearDown() override
    {
        TaskScheduler::instance->close();
        TaskScheduler::instance.reset();
    }
};

TEST_F(EventTestRunner, run)
{
    auto do_test = [](bool with_tasks) {
        PipelineExecStatus exec_status;
        auto event = std::make_shared<SimpleEvent>(exec_status, with_tasks);
        assert(event->isNonDependent());
        event->schedule();
        exec_status.wait();
        auto err_msg = exec_status.getErrMsg();
        ASSERT_TRUE(err_msg.empty()) << err_msg;
    };
    do_test(false);
    do_test(true);
}

TEST_F(EventTestRunner, cancel)
{
    auto do_test = [](bool with_tasks) {
        PipelineExecStatus exec_status;
        auto thread_manager = newThreadManager();
        {
            auto cancel_event = std::make_shared<SimpleCancelEvent>(exec_status, with_tasks);
            auto to_err_event = std::make_shared<ToErrEvent>(exec_status);
            to_err_event->addDependency(cancel_event);
            assert(cancel_event->isNonDependent() && !to_err_event->isNonDependent());
            thread_manager->schedule(false, "test event cancel", [cancel_event]() { cancel_event->schedule(); });
        }
        if (with_tasks)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        exec_status.cancel();
        exec_status.wait();
        auto err_msg = exec_status.getErrMsg();
        ASSERT_TRUE(err_msg.empty()) << err_msg;
        thread_manager->wait();
    };
    do_test(false);
    do_test(true);
}

} // namespace DB::tests
