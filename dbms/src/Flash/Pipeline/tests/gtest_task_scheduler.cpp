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
        : waiter(waiter_)
    {}

    ExecTaskStatus execute()
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

class TaskSchedulerTestRunner : public ::testing::Test
{
public:
    static constexpr size_t thread_num = 5;

protected:
    void TearDown() override
    {
        task_scheduler.close();
    }

protected:
    TaskSchedulerConfig config{thread_num, 1, 1};
    TaskScheduler task_scheduler{config};
};

TEST_F(TaskSchedulerTestRunner, simple_task)
{
    size_t task_num = 10;
    Waiter waiter(task_num);
    std::vector<TaskPtr> tasks;
    for (size_t i = 0; i < task_num; ++i)
        tasks.push_back(std::make_unique<SimpleTask>(waiter));
    task_scheduler.submit(tasks);
    waiter.wait();
}

} // namespace DB::tests
