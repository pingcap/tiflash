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

#include <thread>

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

class IOPriorityTestRunner : public ::testing::Test
{
};

TEST_F(IOPriorityTestRunner, base)
try
{
    IOPriorityQueue queue;

    size_t task_num_per_status = 1000;

    for (size_t i = 0; i < task_num_per_status; ++i)
    {
        queue.submit(std::make_unique<MockIOTask>(true));
    }
    for (size_t i = 0; i < task_num_per_status; ++i)
    {
        queue.submit(std::make_unique<MockIOTask>(false));
    }
    queue.finish();

    TaskPtr task;
    for (size_t i = 0; i < task_num_per_status; ++i)
    {
        ASSERT_TRUE(queue.take(task));
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_OUT);
        FINALIZE_TASK(task);
    }
    for (size_t i = 0; i < task_num_per_status; ++i)
    {
        ASSERT_TRUE(queue.take(task));
        ASSERT_TRUE(task);
        ASSERT_EQ(task->getStatus(), ExecTaskStatus::IO_IN);
        FINALIZE_TASK(task);
    }
    ASSERT_FALSE(queue.take(task));

    // No tasks can be submitted after the queue is finished.
    queue.submit(std::make_unique<MockIOTask>(true));
    ASSERT_FALSE(queue.take(task));
}
CATCH

} // namespace DB::tests
