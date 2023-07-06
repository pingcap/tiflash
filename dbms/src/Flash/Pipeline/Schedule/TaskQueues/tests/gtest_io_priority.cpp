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
    MockIOTask(PipelineExecutorStatus & exec_status_, bool is_io_in)
        : Task(exec_status_)
    {
        task_status = is_io_in ? ExecTaskStatus::IO_IN : ExecTaskStatus::IO_OUT;
    }

    ExecTaskStatus executeImpl() noexcept override { return ExecTaskStatus::FINISHED; }
};
} // namespace

class TestIOPriorityTaskQueue : public ::testing::Test
{
public:
    static constexpr UInt64 time_unit_ns = 100'000'000; // 100ms
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
    PipelineExecutorStatus status;
    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(status, true));
        }
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(status, false));
        }
        queue.finish();
    });
    // wait
    thread_manager->wait();

    // No tasks can be submitted after the queue is finished.
    queue.submit(std::make_unique<MockIOTask>(status, false));
    TaskPtr task;
    ASSERT_FALSE(queue.take(task));
}
CATCH

TEST_F(TestIOPriorityTaskQueue, priority)
try
{
    // in 0 : out 0
    {
        PipelineExecutorStatus status;
        IOPriorityQueue queue;
        queue.submit(std::make_unique<MockIOTask>(status, true));
        queue.submit(std::make_unique<MockIOTask>(status, false));
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
        PipelineExecutorStatus status;
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * IOPriorityQueue::ratio_of_out_to_in);
        queue.submit(std::make_unique<MockIOTask>(status, true));
        queue.submit(std::make_unique<MockIOTask>(status, false));
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
        PipelineExecutorStatus status;
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * (1 + IOPriorityQueue::ratio_of_out_to_in));
        queue.submit(std::make_unique<MockIOTask>(status, true));
        queue.submit(std::make_unique<MockIOTask>(status, false));
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
        PipelineExecutorStatus status;
        IOPriorityQueue queue;
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * (IOPriorityQueue::ratio_of_out_to_in - 1));
        queue.submit(std::make_unique<MockIOTask>(status, true));
        queue.submit(std::make_unique<MockIOTask>(status, false));
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

TEST_F(TestIOPriorityTaskQueue, cancel)
try
{
    // case1 submit first.
    {
        IOPriorityQueue queue;
        PipelineExecutorStatus status1("id1", "", nullptr);
        queue.submit(std::make_unique<MockIOTask>(status1, false));
        PipelineExecutorStatus status2("id2", "", nullptr);
        queue.submit(std::make_unique<MockIOTask>(status2, true));
        queue.cancel("id2");
        TaskPtr task;
        ASSERT_TRUE(!queue.empty());
        queue.take(task);
        ASSERT_EQ(task->getQueryId(), "id2");
        FINALIZE_TASK(task);
        ASSERT_TRUE(!queue.empty());
        queue.take(task);
        ASSERT_EQ(task->getQueryId(), "id1");
        FINALIZE_TASK(task);
    }

    // case2 cancel first.
    {
        IOPriorityQueue queue;
        queue.cancel("id2");
        PipelineExecutorStatus status1("id1", "", nullptr);
        queue.submit(std::make_unique<MockIOTask>(status1, false));
        PipelineExecutorStatus status2("id2", "", nullptr);
        queue.submit(std::make_unique<MockIOTask>(status2, true));
        TaskPtr task;
        ASSERT_TRUE(!queue.empty());
        queue.take(task);
        ASSERT_EQ(task->getQueryId(), "id2");
        FINALIZE_TASK(task);
        ASSERT_TRUE(!queue.empty());
        queue.take(task);
        ASSERT_EQ(task->getQueryId(), "id1");
        FINALIZE_TASK(task);
    }
}
CATCH

} // namespace DB::tests
