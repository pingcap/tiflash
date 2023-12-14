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

#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <common/logger_useful.h>

namespace DB
{
namespace tests
{
TEST(AsyncTasksTest, AsyncTasksNormal)
{
    using namespace std::chrono_literals;
    using TestAsyncTasks = AsyncTasks<uint64_t, std::function<void()>, void>;

    // Lifetime of tasks
    {
        auto async_tasks = std::make_unique<TestAsyncTasks>(1, 1, 1);
        auto sp_after_sched = SyncPointCtl::enableInScope("after_AsyncTasks::addTask_scheduled");
        auto sp_before_quit = SyncPointCtl::enableInScope("before_AsyncTasks::addTask_quit");
        std::thread t1([&]() {
            sp_after_sched.waitAndPause();
            ASSERT_EQ(async_tasks->unsafeQueryState(1), TestAsyncTasks::TaskState::NotScheduled);
            sp_after_sched.next();
            sp_after_sched.disable();
        });
        std::thread t2([&]() {
            sp_before_quit.waitAndPause();
            ASSERT_EQ(async_tasks->unsafeQueryState(1), TestAsyncTasks::TaskState::InQueue);
            sp_before_quit.next();
            sp_before_quit.disable();
            std::this_thread::sleep_for(50ms);
            ASSERT_TRUE(async_tasks->isReady(1));
        });
        auto res = async_tasks->addTask(1, []() {});
        ASSERT_TRUE(res);
        t1.join();
        t2.join();
    }

    // Cancel in queue
    {
        auto async_tasks = std::make_unique<TestAsyncTasks>(1, 1, 2);
        bool finished = false;
        bool canceled = false;
        std::mutex mtx;
        std::unique_lock<std::mutex> cl(mtx);

        auto res1 = async_tasks->addTask(1, [&]() {
            std::scoped_lock rl(mtx);
            UNUSED(rl);
        });
        ASSERT_TRUE(res1);

        auto res2 = async_tasks->addTaskWithCancel(2, [&]() {
            finished = true;
        }, [&]() {
            canceled = true;
        });
        ASSERT_TRUE(res2);

        async_tasks->asyncCancelTask(2);
        cl.unlock();
    
        int elapsed = 0;
        while (true)
        {
            if (canceled)
            {
                break;
            }
            ++elapsed;
            std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(elapsed < 10);
        ASSERT_FALSE(finished);
    }

    // Block cancel
    {
        auto async_tasks = std::make_unique<TestAsyncTasks>(2, 2, 10);
        int total = 9;
        int finished = 0;
        std::vector<bool> f(total, false);
        for (int i = 0; i < total; i++)
        {
            auto res = async_tasks->addTask(i, [i, &async_tasks, &finished]() {
                auto cancel_handle = async_tasks->getCancelHandleFromExecutor(i);
                while (true)
                {
                    std::this_thread::sleep_for(100ms);
                    if (cancel_handle->canceled())
                    {
                        break;
                    }
                }
                finished += 1;
            });
            // Ensure thread 1 is the first
            if (i == 0)
                std::this_thread::sleep_for(10ms);
            ASSERT_TRUE(res);
        }

        while (finished < total)
        {
            std::this_thread::sleep_for(100ms);
            for (int i = 0; i < total; i++)
            {
                if (f[i])
                    continue;
                f[i] = true;
                [[maybe_unused]] auto a = async_tasks->blockedCancelRunningTask(i);
                break;
            }
        }

        ASSERT_EQ(async_tasks->count(), 0);
    }

    // Cancel tasks in queue
    {
        auto async_tasks = std::make_unique<TestAsyncTasks>(1, 1, 100);

        int total = 7;
        std::atomic_int finished = 0;
        for (int i = 0; i < total; i++)
        {
            auto res = async_tasks->addTaskWithCancel(i, [i, &async_tasks, &finished]() {
                while (true)
                {
                    auto cancel_handle = async_tasks->getCancelHandleFromExecutor(i);
                    // Busy loop to take over cpu
                    if (cancel_handle->canceled())
                    {
                        break;
                    }
                }
                finished.fetch_add(1);
            }, [&](){
                finished.fetch_add(1);
            });
            // Ensure task 1 is the first to handle
            if (i == 0)
                std::this_thread::sleep_for(10ms);
            ASSERT_TRUE(res);
        }

        for (int i = 0; i < total; i++)
        {
            std::this_thread::sleep_for(100ms);
            async_tasks->asyncCancelTask(i);
            // Throw on double cancel
            EXPECT_THROW(async_tasks->asyncCancelTask(i), Exception);
        }

        int elapsed = 0;
        while (true)
        {
            if (finished >= total)
            {
                break;
            }
            ++elapsed;
            std::this_thread::sleep_for(100ms);
        }
        ASSERT_TRUE(elapsed < 50);
        ASSERT_EQ(async_tasks->count(), 0);
    }
}

TEST(AsyncTasksTest, AsyncTasksCommon)
{
    using namespace std::chrono_literals;

    using TestAsyncTasks = AsyncTasks<uint64_t, std::function<int()>, int>;
    auto async_tasks = std::make_unique<TestAsyncTasks>(1, 1, 2);

    int total = 5;
    int max_steps = 10;
    int current_step = 0;
    std::vector<bool> f(total, false);
    std::vector<bool> s(total, false);
    bool initial_loop = true;
    while (true)
    {
        ASSERT(current_step < max_steps);
        SCOPE_EXIT({ initial_loop = false; });
        auto count = std::accumulate(f.begin(), f.end(), 0, [&](int a, bool b) -> int { return a + int(b); });
        if (count >= total)
        {
            break;
        }

        auto to_be_canceled = total - 1;
        if (count == total - 1)
        {
            if (async_tasks->isScheduled(to_be_canceled))
            {
                async_tasks->asyncCancelTask(
                    to_be_canceled,
                    []() {},
                    true);
            }
            // Otherwise, the task is not added.
        }

        // Add tasks
        for (int i = 0; i < total; ++i)
        {
            if (!async_tasks->isScheduled(i) && !s[i])
            {
                auto res = async_tasks->addTask(i, [i, &async_tasks, to_be_canceled, &f]() {
                    if (i == to_be_canceled)
                    {
                        auto cancel_handle = async_tasks->getCancelHandleFromExecutor(i);
                        while (true)
                        {
                            if (cancel_handle->blockedWaitFor(100ms))
                            {
                                f[to_be_canceled] = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        std::this_thread::sleep_for(100ms);
                    }
                    return 1;
                });
                if (res)
                    s[i] = true;
                // In the first loop, only the first task can run.
                if (initial_loop)
                    ASSERT_EQ(res, i <= 1);
            }
        }

        // Fetch result
        for (int i = 0; i < total; ++i)
        {
            if (!f[i])
            {
                if (i == to_be_canceled)
                    continue;
                if (async_tasks->isReady(i))
                {
                    auto r = async_tasks->fetchResult(i);
                    UNUSED(r);
                    f[i] = true;
                }
            }
        }
        std::this_thread::sleep_for(100ms);
    }

    ASSERT_EQ(async_tasks->count(), 0);
}
} // namespace tests
} // namespace DB