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

#include <Flash/Pipeline/TaskScheduler.h>
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
        std::unique_lock lock(mu);
        while (0 != counter)
            cv.wait(lock);
    }

private:
    int64_t counter;
    mutable std::mutex mu;
    std::condition_variable cv;
};

class SimpleTask : public Task
{
public:
    SimpleTask(Waiter & waiter_)
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
    SimpleWaitingTask(Waiter & waiter_)
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
    SimpleSpillingTask(Waiter & waiter_)
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
} // namespace

class TaskSchedulerTestRunner : public ::testing::Test
{
public:
    static constexpr size_t thread_num = 5;
};

TEST_F(TaskSchedulerTestRunner, simple_task)
{
    size_t task_num = 10;
    Waiter waiter(task_num);
    std::vector<TaskPtr> tasks;
    for (size_t i = 0; i < task_num; ++i)
        tasks.push_back(std::make_unique<SimpleTask>(waiter));
    TaskSchedulerConfig config{thread_num, 0};
    TaskScheduler task_scheduler{config};
    task_scheduler.submit(tasks);
    waiter.wait();
    task_scheduler.close();
}

TEST_F(TaskSchedulerTestRunner, simple_waiting_task)
{
    size_t task_num = 10;
    Waiter waiter(task_num);
    std::vector<TaskPtr> tasks;
    for (size_t i = 0; i < task_num; ++i)
        tasks.push_back(std::make_unique<SimpleWaitingTask>(waiter));
    TaskSchedulerConfig config{thread_num, 0};
    TaskScheduler task_scheduler{config};
    task_scheduler.submit(tasks);
    waiter.wait();
    task_scheduler.close();
}

TEST_F(TaskSchedulerTestRunner, simple_spilling_task)
{
    auto test = [](size_t spiller_executor_thread_num) {
        size_t task_num = 10;
        Waiter waiter(task_num);
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
            tasks.push_back(std::make_unique<SimpleSpillingTask>(waiter));
        TaskSchedulerConfig config{thread_num, spiller_executor_thread_num};
        TaskScheduler task_scheduler{config};
        task_scheduler.submit(tasks);
        waiter.wait();
        task_scheduler.close();
    };
    test(0);
    test(thread_num);
}

} // namespace DB::tests
