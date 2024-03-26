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
#include <Flash/Pipeline/Schedule/Reactor/WaitingTaskList.h>
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

    ReturnStatus executeImpl() noexcept override { return ExecTaskStatus::FINISHED; }
};
} // namespace

class TestWaitingTaskList : public ::testing::Test
{
};

TEST_F(TestWaitingTaskList, base)
try
{
    PipelineExecutorContext context;
    // To avoid the active ref count being returned to 0 in advance.
    context.incActiveRefCount();
    SCOPE_EXIT({ context.decActiveRefCount(); });

    WaitingTaskList list;

    auto thread_manager = newThreadManager();
    size_t round = 1000;
    size_t valid_task_num = 2 * round;

    // take/tryTask valid task
    std::atomic_size_t taken_task_num = 0;
    auto take_func = [&](bool is_try) {
        std::list<TaskPtr> local_list;
        while (is_try ? list.tryTake(local_list) : list.take(local_list))
        {
            for (auto & task : local_list)
            {
                ASSERT_TRUE(task);
                ++taken_task_num;
                FINALIZE_TASK(task);
            }
            local_list.clear();
        }
    };
    thread_manager->schedule(false, "take", [&]() { take_func(false); });
    thread_manager->schedule(false, "try_take", [&]() { take_func(true); });
    // submit valid task
    for (size_t i = 0; i < round; ++i)
    {
        list.submit(std::make_unique<PlainTask>(context));
        std::list<TaskPtr> local_list;
        local_list.push_back(std::make_unique<PlainTask>(context));
        list.submit(local_list);
    }
    list.finish();
    // wait
    thread_manager->wait();
    ASSERT_EQ(taken_task_num, valid_task_num);

    // No tasks are submitted after the list is finished.
    list.submit(std::make_unique<PlainTask>(context));
    {
        std::list<TaskPtr> local_list;
        ASSERT_FALSE(list.take(local_list));
        ASSERT_FALSE(list.tryTake(local_list));
    }
}
CATCH

} // namespace DB::tests
