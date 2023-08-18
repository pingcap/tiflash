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
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
class PlainTask : public Task
{
public:
    explicit PlainTask(PipelineExecutorContext & exec_context_)
        : Task(exec_context_)
    {}

    ExecTaskStatus executeImpl() noexcept override { return ExecTaskStatus::FINISHED; }
};
} // namespace

class TestMLFQTaskQueue : public ::testing::Test
{
};

// mlfq
TEST_F(TestMLFQTaskQueue, init)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({ context.decActiveRefCount(); });

    TaskQueuePtr queue = std::make_unique<CPUMultiLevelFeedbackQueue>();
    size_t valid_task_num = 1000;
    // submit
    for (size_t i = 0; i < valid_task_num; ++i)
        queue->submit(std::make_unique<PlainTask>(context));
    // take
    for (size_t i = 0; i < valid_task_num; ++i)
    {
        TaskPtr task;
        queue->take(task);
        ASSERT_EQ(task->mlfq_level, 0);
        FINALIZE_TASK(task);
    }
    ASSERT_TRUE(queue->empty());
    queue->finish();
    // No tasks can be submitted after the queue is finished.
    queue->submit(std::make_unique<PlainTask>(context));
    TaskPtr task;
    ASSERT_FALSE(queue->take(task));
}
CATCH

TEST_F(TestMLFQTaskQueue, random)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({ context.decActiveRefCount(); });

    TaskQueuePtr queue = std::make_unique<CPUMultiLevelFeedbackQueue>();

    auto thread_manager = newThreadManager();
    size_t valid_task_num = 1000;
    auto mock_value = []() {
        return CPUMultiLevelFeedbackQueue::LEVEL_TIME_SLICE_BASE_NS * (1 + random() % 100);
    };

    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < valid_task_num; ++i)
        {
            TaskPtr task = std::make_unique<PlainTask>(context);
            auto value = mock_value();
            queue->updateStatistics(task, ExecTaskStatus::RUNNING, value);
            task->profile_info.addCPUExecuteTime(value);
            queue->submit(std::move(task));
        }
        while (!queue->empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        queue->finish();
    });
    // take valid task
    thread_manager->schedule(false, "take", [&]() {
        size_t take_task_num = 0;
        TaskPtr task;
        while (queue->take(task))
        {
            ASSERT_TRUE(task);
            ++take_task_num;
            FINALIZE_TASK(task);
        }
        ASSERT_EQ(take_task_num, valid_task_num);
    });
    thread_manager->wait();
}
CATCH

TEST_F(TestMLFQTaskQueue, level)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({ context.decActiveRefCount(); });

    CPUMultiLevelFeedbackQueue queue;
    TaskPtr task = std::make_unique<PlainTask>(context);
    queue.submit(std::move(task));
    for (size_t level = 0; level < CPUMultiLevelFeedbackQueue::QUEUE_SIZE; ++level)
    {
        while (queue.take(task))
        {
            ASSERT_EQ(task->mlfq_level, level);
            ASSERT_TRUE(task);
            auto value = CPUMultiLevelFeedbackQueue::LEVEL_TIME_SLICE_BASE_NS;
            queue.updateStatistics(task, ExecTaskStatus::RUNNING, value);
            task->profile_info.addCPUExecuteTime(value);
            bool need_break = CPUTimeGetter::get(task) >= queue.getUnitQueueInfo(level).time_slice;
            queue.submit(std::move(task));
            if (need_break)
                break;
        }
    }
    queue.take(task);
    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(task->mlfq_level, CPUMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
    FINALIZE_TASK(task);
    queue.finish();
}
CATCH

TEST_F(TestMLFQTaskQueue, feedback)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({ context.decActiveRefCount(); });

    CPUMultiLevelFeedbackQueue queue;

    // The case that low level > high level
    {
        // level `QUEUE_SIZE - 1`
        TaskPtr task = std::make_unique<PlainTask>(context);
        task->mlfq_level = CPUMultiLevelFeedbackQueue::QUEUE_SIZE - 1;
        auto value = queue.getUnitQueueInfo(task->mlfq_level).time_slice;
        queue.updateStatistics(task, ExecTaskStatus::RUNNING, value);
        task->profile_info.addCPUExecuteTime(value);
        queue.submit(std::move(task));
    }
    {
        // level `0`
        TaskPtr task = std::make_unique<PlainTask>(context);
        auto value = queue.getUnitQueueInfo(0).time_slice - 1;
        queue.updateStatistics(task, ExecTaskStatus::RUNNING, value);
        task->profile_info.addCPUExecuteTime(value);
        queue.submit(std::move(task));
    }
    // the first task will be level `0`.
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, 0);
        FINALIZE_TASK(task);
    }
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, CPUMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
        FINALIZE_TASK(task);
    }
    ASSERT_TRUE(queue.empty());

    // The case that low level < high level
    size_t task_num = 1000;
    for (size_t i = 0; i < task_num; ++i)
    {
        // level `0`
        TaskPtr task = std::make_unique<PlainTask>(context);
        auto value = queue.getUnitQueueInfo(0).time_slice - 1;
        queue.updateStatistics(task, ExecTaskStatus::RUNNING, value);
        task->profile_info.addCPUExecuteTime(value);
        queue.submit(std::move(task));
    }
    {
        // level `QUEUE_SIZE - 1`
        TaskPtr task = std::make_unique<PlainTask>(context);
        task->mlfq_level = CPUMultiLevelFeedbackQueue::QUEUE_SIZE - 1;
        auto value = queue.getUnitQueueInfo(task->mlfq_level).time_slice;
        queue.updateStatistics(task, ExecTaskStatus::RUNNING, value);
        task->profile_info.addCPUExecuteTime(value);
        queue.submit(std::move(task));
    }
    // the first task will be level `QUEUE_SIZE - 1`.
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, CPUMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
        FINALIZE_TASK(task);
    }
    for (size_t i = 0; i < task_num; ++i)
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, 0);
        FINALIZE_TASK(task);
    }
    ASSERT_TRUE(queue.empty());
    queue.finish();
}
CATCH

TEST_F(TestMLFQTaskQueue, cancel)
try
{
    PipelineExecutorContext context1("id1", "", nullptr);
    // To avoid the active ref count being returned to 0 in advance.
    context1.incActiveRefCount();
    SCOPE_EXIT({ context1.decActiveRefCount(); });

    PipelineExecutorContext context2("id2", "", nullptr);
    // To avoid the active ref count being returned to 0 in advance.
    context2.incActiveRefCount();
    SCOPE_EXIT({ context2.decActiveRefCount(); });

    // case1 submit first.
    {
        CPUMultiLevelFeedbackQueue queue;
        queue.submit(std::make_unique<PlainTask>(context1));
        queue.submit(std::make_unique<PlainTask>(context2));
        queue.cancel("id2", "");
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
        CPUMultiLevelFeedbackQueue queue;
        queue.cancel("id2", "");
        queue.submit(std::make_unique<PlainTask>(context1));
        queue.submit(std::make_unique<PlainTask>(context2));
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
