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

#pragma once

#include <Common/UniThreadPool.h>

#include <future>

namespace DB
{
template <typename Key, typename Func, typename R>
struct AsyncTasks
{
    // We use a big queue to cache, to reduce add task failures.
    explicit AsyncTasks(uint64_t pool_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, pool_size, 300))
    {}
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
    {}

    bool addTask(Key k, Func f)
    {
        using P = std::packaged_task<R()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));

        auto res = thread_pool->trySchedule([p]() { (*p)(); }, 0, 0);
        if (res)
        {
            std::scoped_lock l(mtx);
            futures[k] = p->get_future();
            start_time[k] = getCurrentMillis();
        }
        return res;
    }

    bool isScheduled(Key key) const
    {
        std::scoped_lock l(mtx);
        return futures.count(key);
    }

    bool isReady(Key key) const
    {
        using namespace std::chrono_literals;
        std::scoped_lock l(mtx);
        if (!futures.count(key))
            return false;
        return futures.at(key).wait_for(0ms) == std::future_status::ready;
    }

    R fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = futures.find(key);
        RUNTIME_CHECK_MSG(it != futures.end(), "fetchResult is empty");
        auto fut = std::move(it->second);
        futures.erase(it);
        start_time.erase(key);
        l.unlock();
        return fut.get();
    }

    std::pair<R, uint64_t> fetchResultAndElapsed(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = futures.find(key);
        auto fut = std::move(it->second);
        auto it2 = start_time.find(key);
        RUNTIME_CHECK_MSG(it != futures.end() && it2 != start_time.end(), "fetchResultAndElapsed is empty");
        auto start = it2->second;
        futures.erase(it);
        start_time.erase(it2);
        l.unlock();
        auto elapsed = getCurrentMillis() - start;
        return std::make_pair(fut.get(), elapsed);
    }

    std::unique_ptr<ThreadPool> & inner() { return thread_pool; }

private:
    static uint64_t getCurrentMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

protected:
    std::map<Key, std::future<R>> futures;
    std::map<Key, uint64_t> start_time;
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
};
} // namespace DB