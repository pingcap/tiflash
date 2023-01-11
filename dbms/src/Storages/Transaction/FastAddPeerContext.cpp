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

#include <Storages/Transaction/ProxyFFI.h>

#include <memory>
#include <mutex>

#include "FastAddPeer.h"
namespace DB
{
FastAddPeerContext::FastAddPeerContext()
{
    static constexpr int ffi_handle_sec = 5;
    static constexpr int region_per_sec = 2;
    int thread_count = ffi_handle_sec * region_per_sec;
    tasks_trace = new AsyncTasks(thread_count);
}

FastAddPeerContext::~FastAddPeerContext()
{
    delete tasks_trace;
}

bool FastAddPeerContext::AsyncTasks::addTask(Key k, Func f)
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

bool FastAddPeerContext::AsyncTasks::isScheduled(Key key) const
{
    std::scoped_lock l(mtx);
    return futures.count(key);
}

bool FastAddPeerContext::AsyncTasks::isReady(Key key) const
{
    using namespace std::chrono_literals;
    std::scoped_lock l(mtx);
    if (!futures.count(key))
        return false;
    if (futures.at(key).wait_for(0ms) == std::future_status::ready)
    {
        return true;
    }
    return false;
}

FastAddPeerRes FastAddPeerContext::AsyncTasks::fetchResult(Key key)
{
    std::unique_lock<std::mutex> l(mtx);
    auto it = futures.find(key);
    auto fut = std::move(it->second);
    futures.erase(it);
    l.unlock();
    return fut.get();
}
} // namespace DB
