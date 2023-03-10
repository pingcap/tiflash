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
#include <Flash/Pipeline/Schedule/TaskQueues/FiFOTaskQueue.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <thread>

namespace DB::tests
{
namespace
{
class IndexTask : public Task
{
public:
    explicit IndexTask(size_t index_)
        : index(index_)
    {}

    ExecTaskStatus executeImpl() noexcept override { return ExecTaskStatus::FINISHED; }

    size_t index;
};
} // namespace

class FIFOTestRunner : public ::testing::Test
{
};

TEST_F(FIFOTestRunner, base)
try
{
    FIFOTaskQueue queue;

    auto thread_manager = newThreadManager();
    size_t valid_task_num = 1000;

    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < valid_task_num; ++i)
            queue.submit(std::make_unique<IndexTask>(i));
        // Close the queue after all valid tasks have been consumed.
        while (!queue.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue.close();
    });
    // take valid task
    thread_manager->schedule(false, "take", [&]() {
        TaskPtr task;
        size_t expect_index = 0;
        while (queue.take(task))
        {
            ASSERT_TRUE(task);
            auto * index_task = static_cast<IndexTask *>(task.get());
            ASSERT_EQ(index_task->index, expect_index++);
            task.reset();
        }
        ASSERT_EQ(expect_index, valid_task_num);
    });
    thread_manager->wait();

    // No tasks are taken after the queue is closed.
    queue.submit(std::make_unique<IndexTask>(valid_task_num));
    TaskPtr task;
    ASSERT_FALSE(queue.take(task));
}
CATCH

} // namespace DB::tests
