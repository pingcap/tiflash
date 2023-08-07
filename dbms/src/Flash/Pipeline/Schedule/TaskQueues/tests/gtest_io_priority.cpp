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
#include <Flash/Executor/PipelineExecutorContext.h>
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
    MockIOTask(PipelineExecutorContext & exec_context_, bool is_io_in)
        : Task(exec_context_, "", is_io_in ? ExecTaskStatus::IO_IN : ExecTaskStatus::IO_OUT)
    {
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
    PipelineExecutorContext context;

    IOPriorityQueue queue;

    auto thread_manager = newThreadManager();
    size_t task_num_per_status = 1000;

    // take valid task
    size_t taken_take_num = 0;
    thread_manager->schedule(false, "take", [&]() {
        TaskPtr task;
        while (queue.take(task))
        {
            ASSERT_TRUE(task);
            ++taken_take_num;
            FINALIZE_TASK(task);
        }
    });
    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(context, true));
        }
        for (size_t i = 0; i < task_num_per_status; ++i)
        {
            queue.submit(std::make_unique<MockIOTask>(context, false));
        }
        queue.finish();
    });

    // 10 seconds is totally enough for 1000 SimpleTask to run.
    // When waitFor() returns, it means all tasks runs finish and finilize() is called.
    context.waitFor(std::chrono::seconds(10));

    // Some tasks will not be taken successfully, because queue is already finished.
    ASSERT_LE(taken_take_num, 2 * task_num_per_status);

    // wait
    thread_manager->wait();

    // No tasks can be submitted after the queue is finished.
    context.incActiveRefCount();
    queue.submit(std::make_unique<MockIOTask>(context, false));
    TaskPtr task;
    ASSERT_FALSE(queue.take(task));
}
CATCH

TEST_F(TestIOPriorityTaskQueue, priority)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({
        context.decActiveRefCount();
    });

    // in 0 : out 0
    {
        IOPriorityQueue queue;
        queue.submit(std::make_unique<MockIOTask>(context, true));
        queue.submit(std::make_unique<MockIOTask>(context, false));
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
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * IOPriorityQueue::ratio_of_out_to_in);
        queue.submit(std::make_unique<MockIOTask>(context, true));
        queue.submit(std::make_unique<MockIOTask>(context, false));
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
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * (1 + IOPriorityQueue::ratio_of_out_to_in));
        queue.submit(std::make_unique<MockIOTask>(context, true));
        queue.submit(std::make_unique<MockIOTask>(context, false));
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
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_IN, time_unit_ns);
        queue.updateStatistics(nullptr, ExecTaskStatus::IO_OUT, time_unit_ns * (IOPriorityQueue::ratio_of_out_to_in - 1));
        queue.submit(std::make_unique<MockIOTask>(context, true));
        queue.submit(std::make_unique<MockIOTask>(context, false));
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
    PipelineExecutorContext context1("id1", "", nullptr);
    // To avoid the active ref count being returned to 0 in advance.
    context1.incActiveRefCount();
    SCOPE_EXIT({
        context1.decActiveRefCount();
    });

    PipelineExecutorContext context2("id2", "", nullptr);
    // To avoid the active ref count being returned to 0 in advance.
    context2.incActiveRefCount();
    SCOPE_EXIT({
        context2.decActiveRefCount();
    });

    // case1 submit first.
    {
        IOPriorityQueue queue;
        queue.submit(std::make_unique<MockIOTask>(context1, false));
        queue.submit(std::make_unique<MockIOTask>(context2, true));
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
        queue.submit(std::make_unique<MockIOTask>(context1, false));
        queue.submit(std::make_unique<MockIOTask>(context2, true));
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
