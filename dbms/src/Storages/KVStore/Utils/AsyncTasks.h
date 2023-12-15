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
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
    {}

    bool discardTask(Key k)
    {
        std::scoped_lock l(mtx);
        auto it = futures.find(k);
        if (it != futures.end())
        {
            futures.erase(it);
            start_time.erase(k);
            return true;
        }
        return false;
    }

    bool addTask(Key k, Func f)
    {
        using P = std::packaged_task<R()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));

        // TODO(fap) `start_time` may not be set immediately when calling `p`, will be fixed in another PR.
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
        return futures.contains(key);
    }

    bool isReady(Key key) const
    {
        using namespace std::chrono_literals;
        std::scoped_lock l(mtx);
        auto it = futures.find(key);
        if (it == futures.end())
            return false;
        return it->second.wait_for(0ms) == std::future_status::ready;
    }

    R fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = futures.find(key);
        RUNTIME_CHECK_MSG(it != futures.end(), "fetchResult meets empty key");
        auto fut = std::move(it->second);
        futures.erase(it);
        start_time.erase(key);
        l.unlock();
        return fut.get();
    }

    uint64_t queryElapsed(Key key)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it2 = start_time.find(key);
        RUNTIME_CHECK_MSG(it2 != start_time.end(), "queryElapsed meets empty key");
        return getCurrentMillis() - it2->second;
    }

    uint64_t queryStartTime(Key key)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it2 = start_time.find(key);
        RUNTIME_CHECK_MSG(it2 != start_time.end(), "queryStartTime meets empty key");
        return it2->second;
    }

    std::pair<R, uint64_t> fetchResultAndElapsed(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = futures.find(key);
        auto fut = std::move(it->second);
        auto it2 = start_time.find(key);
        RUNTIME_CHECK_MSG(it != futures.end() && it2 != start_time.end(), "fetchResultAndElapsed meets empty key");
        auto start = it2->second;
        futures.erase(it);
        start_time.erase(it2);
        l.unlock();
        auto elapsed = getCurrentMillis() - start;
        return std::make_pair(fut.get(), elapsed);
    }

    std::unique_ptr<ThreadPool> & inner() { return thread_pool; }

    static uint64_t getCurrentMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

protected:
    std::unordered_map<Key, std::future<R>> futures;
    std::unordered_map<Key, uint64_t> start_time;
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
};
} // namespace DB