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
#include <magic_enum.hpp>
#include <Common/Logger.h>
#include <future>

namespace DB
{
template <typename Key, typename Func, typename R>
struct AsyncTasks
{
    // We use a big queue to cache, to reduce add task failures.
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
    {
        log = DB::Logger::get("AsyncTasks");
    }

    struct CancelHandle {
        CancelHandle() = default;
        CancelHandle(const CancelHandle &) = delete;

        bool canceled() const { return inner->load(); }

        void doCancel() {
            // Use lock here to prevent losing signal.
            try {
                std::unique_lock<std::mutex> lock(mut);
                LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! doCancel 1");
                inner->store(true);
                cv.notify_all();
                LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! doCancel 2");
            }
            catch (...)
            {
                tryLogCurrentException(DB::Logger::get(), "doCancel fail");
                exit(-1);
            }

        }
        bool blockedWaitFor(std::chrono::duration<double, std::milli> timeout) {
            try {
                LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! blockedWaitFor 1");
                std::unique_lock<std::mutex> lock(mut);
                cv.wait_for(lock, timeout, [&](){ return canceled(); } );
                LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! blockedWaitFor 2");
                return canceled();
            }
            catch (...)
            {
                tryLogCurrentException(DB::Logger::get(), "blockedWaitFor fail");
                exit(-1);
            }
        }
    private:
        std::shared_ptr<std::atomic_bool> inner = std::make_shared<std::atomic_bool>(false);
        std::mutex mut;
        std::condition_variable cv;
    };

    struct Elem {
        Elem(std::future<R> && fut_, uint64_t start_ts_) 
            : fut(std::move(fut_)), start_ts(start_ts_)
        {
            cancel = std::make_shared<CancelHandle>();
        }
        Elem(const Elem &) = delete;
        Elem(Elem &&) = default;

        std::future<R> fut;
        uint64_t start_ts;
        std::shared_ptr<CancelHandle> cancel;
    };

    std::shared_ptr<CancelHandle> getCancelHandle(Key k) const {
        std::unique_lock<std::mutex> l(mtx);
        auto it = tasks.find(k);
        RUNTIME_CHECK_MSG(it != tasks.end(), "fetchResult meets empty key");
        return it->second.cancel;
    }

    bool discardTask(Key k)
    {
        std::scoped_lock l(mtx);
        auto it = tasks.find(k);
        if (it != tasks.end())
        {
            tasks.erase(it);
            return true;
        }
        return false;
    }

    bool addTask(Key k, Func f)
    {
        std::scoped_lock l(mtx);
        using P = std::packaged_task<R()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));

        auto res = thread_pool->trySchedule([p]() { (*p)(); }, 0, 0);
        if (res)
        {
            tasks.insert({k, Elem (p->get_future(), getCurrentMillis())});
        }
        return res;
    }

    bool isScheduled(Key key) const
    {
        std::scoped_lock l(mtx);
        return tasks.contains(key);
    }

    bool isReady(Key key) const
    {
        using namespace std::chrono_literals;
        std::scoped_lock l(mtx);
        auto it = tasks.find(key);
        if (it == tasks.end())
            return false;
        LOG_INFO(log, "!!!!! XXX {}", magic_enum::enum_name(it->second.fut.wait_for(0ms)));
        return it->second.fut.wait_for(0ms) == std::future_status::ready;
    }

    R fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "fetchResult meets empty key");
        std::future<R> fut = std::move(it->second.fut);
        LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! fetchResult 1 {}", key);
        tasks.erase(key);
        l.unlock();
        RUNTIME_CHECK_MSG(fut.valid(), "no valid future");
        LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! fetchResult 2 {}", key);
        return fut.get();
        // try {
        //     return fut.get();
        // } catch (std::exception & e)
        // {
        //     LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! a {}", e.what());
        //     exit(0);
        // } catch (Exception & e) {
        //     LOG_INFO(&Poco::Logger::get("!!!!!! a"), "!!! b {} {}", e.what(), e.message());
        //     exit(0);
        // }
    }

    uint64_t queryElapsed(Key key)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "queryElapsed meets empty key");
        return getCurrentMillis() - it->second.start_ts;
    }

    uint64_t queryStartTime(Key key)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "queryElapsed meets empty key");
        return it->second.start_ts;
    }

    std::pair<R, uint64_t> fetchResultAndElapsed(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "fetchResultAndElapsed meets empty key");
        auto fut = std::move(it->second.fut);
        auto start = it->second.start_ts;
        tasks.erase(it);
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
    std::unordered_map<Key, Elem> tasks;
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
    LoggerPtr log;
};
} // namespace DB