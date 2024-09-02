// Copyright 2024 PingCAP, Inc.
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


#include <IO/IOThreadPools.h>
#include <gtest/gtest.h>

#include <exception>
#include <ext/scope_guard.h>
#include <future>
#include <random>

namespace DB::tests
{
namespace
{
using SPtr = std::shared_ptr<std::atomic<int>>;
using WPtr = std::weak_ptr<std::atomic<int>>;

void buildReadTasks(bool throw_in_build_read_tasks, bool throw_in_build_task, WPtr wp, SPtr invalid_count)
{
    auto async_build_read_task = [=]() {
        return BuildReadTaskPool::get().scheduleWithFuture([=]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            auto sleep_ms = gen() % 100 + 1; // 1~100
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

            if (auto sp = wp.lock(); sp)
                sp->fetch_add(1);
            else
                invalid_count->fetch_add(1);

            if (throw_in_build_task)
                throw Exception("From build_read_task");
        });
    };

    IOPoolHelper::FutureContainer futures(Logger::get("buildReadTasks"));
    for (int i = 0; i < 10; i++)
    {
        futures.add(async_build_read_task());
        if (i >= 5 && throw_in_build_read_tasks)
            throw Exception("From buildReadTasks");
    }
    futures.getAllResults();
}

void buildReadTasksForTables(
    bool throw_in_build_read_tasks_for_tables,
    bool throw_in_build_read_tasks,
    bool throw_in_build_task,
    WPtr wp,
    SPtr invalid_count)
{
    auto async_build_read_tasks_for_table = [=]() {
        return BuildReadTaskForWNTablePool::get().scheduleWithFuture(
            [=]() { buildReadTasks(throw_in_build_read_tasks, throw_in_build_task, wp, invalid_count); });
    };

    IOPoolHelper::FutureContainer futures(Logger::get("buildReadTasksForTables"));
    for (int i = 0; i < 10; i++)
    {
        futures.add(async_build_read_tasks_for_table());
        if (i >= 5 && throw_in_build_read_tasks_for_tables)
            throw Exception("From buildReadTasksForTables");
    }
    futures.getAllResults();
}

void buildReadTasksForWNs(
    bool throw_in_build_read_tasks_for_wns,
    bool throw_in_build_read_tasks_for_tables,
    bool throw_in_build_read_tasks,
    bool throw_in_build_task)
{
    auto log = Logger::get("buildReadTasksForWNs");
    LOG_INFO(
        log,
        "throw_in_build_read_tasks_for_wns={}, "
        "throw_in_build_read_tasks_for_tables={}, throw_in_build_read_tasks={}, throw_in_build_task={}",
        throw_in_build_read_tasks_for_wns,
        throw_in_build_read_tasks_for_tables,
        throw_in_build_read_tasks,
        throw_in_build_task);
    auto sp = std::make_shared<std::atomic<int>>(0);
    auto invalid_count = std::make_shared<std::atomic<int>>(0);
    // Use weak_ptr to simulate capture by reference.
    auto async_build_tasks_for_wn = [wp = WPtr{sp},
                                     invalid_count,
                                     throw_in_build_read_tasks_for_tables,
                                     throw_in_build_read_tasks,
                                     throw_in_build_task]() {
        return BuildReadTaskForWNPool::get().scheduleWithFuture([=]() {
            buildReadTasksForTables(
                throw_in_build_read_tasks_for_tables,
                throw_in_build_read_tasks,
                throw_in_build_task,
                wp,
                invalid_count);
        });
    };

    try
    {
        IOPoolHelper::FutureContainer futures(log);
        SCOPE_EXIT({
            if (!throw_in_build_read_tasks_for_wns && !throw_in_build_read_tasks_for_tables
                && !throw_in_build_read_tasks)
                ASSERT_EQ(*sp, 10 * 10 * 10);
            ASSERT_EQ(*invalid_count, 0);
        });
        for (int i = 0; i < 10; i++)
        {
            futures.add(async_build_tasks_for_wn());
            if (i >= 5 && throw_in_build_read_tasks_for_wns)
                throw Exception("From buildReadTasksForWNs");
        }
        futures.getAllResults();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}
} // namespace

TEST(IOThreadPool, TaskChain)
{
    constexpr std::array<bool, 2> arr{false, true};
    for (auto a : arr)
        for (auto b : arr)
            for (auto c : arr)
                for (auto d : arr)
                    buildReadTasksForWNs(a, b, c, d);
}

} // namespace DB::tests
