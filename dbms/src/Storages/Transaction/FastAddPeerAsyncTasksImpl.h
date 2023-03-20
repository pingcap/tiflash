// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/Transaction/FastAddPeer.h>
#include <Storages/Transaction/ProxyFFI.h>

#include <future>

namespace DB
{
struct AsyncTasks
{
    using Key = uint64_t;
    using Func = std::function<FastAddPeerRes()>;

    // We use a big queue to cache, to reduce ass task failures.
    explicit AsyncTasks(uint64_t pool_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, pool_size, 300))
    {}
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
    {}

    bool addTask(Key k, Func f)
    {
        using P = std::packaged_task<FastAddPeerRes()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));

        auto res = thread_pool->trySchedule([p]() { (*p)(); }, 0, 0);
        if (res)
        {
            std::scoped_lock l(mtx);
            futures[k] = p->get_future();
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

    FastAddPeerRes fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = futures.find(key);
        auto fut = std::move(it->second);
        futures.erase(it);
        l.unlock();
        return fut.get();
    }

protected:
    std::map<Key, std::future<FastAddPeerRes>> futures;
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
};
} // namespace DB