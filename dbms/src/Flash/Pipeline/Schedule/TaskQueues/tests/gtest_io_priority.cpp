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
#include <Flash/Pipeline/Schedule/TaskQueues/IOPriorityQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class MockIOTask : public Task
{
public:
    explicit MockIOTask(bool is_io_in)
    {
        task_status = is_io_in ? ExecTaskStatus::IO_IN : ExecTaskStatus::IO_OUT;
    }

    ExecTaskStatus executeImpl() noexcept override { return ExecTaskStatus::FINISHED; }
};
} // namespace

class TestIOPriorityTaskQueue : public ::testing::Test
{
};

TEST_F(TestIOPriorityTaskQueue, base)
try
{
    IOPriorityQueue queue;

    auto thread_manager = newThreadManager();
    size_t task_num_per_status = 1000;

    // take valid task
    thread_manager->schedule(false, "take", [&]() {
        TaskPtr task;
        size_t taken_take_num = 0;
        while (queue.take(task))
        {
            ASSERT_TRUE(task);
            ++taken_take_num;
            FINALIZE_TASK(task);
        }
        ASSERT_EQ(taken_take_num, 2 * task_num_per_status);
    });
    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(true));
        }
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(false));
        }
        queue.finish();
    });
    // wait
    thread_manager->wait();

    // No tasks can be submitted after the queue is finished.
    queue.submit(std::make_unique<MockIOTask>(false));
    TaskPtr task;
    ASSERT_FALSE(queue.take(task));
}
CATCH

TEST_F(TestIOPriorityTaskQueue, priority)
try
{
    // in 0 : out 0
    {
        IOPriorityQueue queue;
        queue.submit(std::make_unique<MockIOTask>(true));
        queue.submit(std::make_unique<MockIOTask>(false));
        TaskPtr task;
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_OUT);
        FINALIZE_TASK(task);
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_IN);
        FINALIZE_TASK(task);
    }

    // in 1 : out ratio_of_in_to_out
    {
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, 1000);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, 1000 * IOPriorityQueue::ratio_of_in_to_out);
        queue.submit(std::make_unique<MockIOTask>(true));
        queue.submit(std::make_unique<MockIOTask>(false));
        TaskPtr task;
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_OUT);
        FINALIZE_TASK(task);
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_IN);
        FINALIZE_TASK(task);
    }

    // in 1 : out ratio_of_in_to_out+1
    {
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, 1000);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, 1000 * (1 + IOPriorityQueue::ratio_of_in_to_out));
        queue.submit(std::make_unique<MockIOTask>(true));
        queue.submit(std::make_unique<MockIOTask>(false));
        TaskPtr task;
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_IN);
        FINALIZE_TASK(task);
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_OUT);
        FINALIZE_TASK(task);
    }

    // in 1 : out ratio_of_in_to_out-1
    {
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, 1000);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, 1000 * (IOPriorityQueue::ratio_of_in_to_out - 1));
        queue.submit(std::make_unique<MockIOTask>(true));
        queue.submit(std::make_unique<MockIOTask>(false));
        TaskPtr task;
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_OUT);
        FINALIZE_TASK(task);
        queue.take(task);
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_IN);
        FINALIZE_TASK(task);
    }
}
CATCH

} // namespace DB::tests
