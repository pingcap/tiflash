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

#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/UniThreadPool.h>

#include <future>
#include <magic_enum.hpp>
#include <unordered_map>

namespace DB
{
template <typename Key, typename Func, typename R>
struct AsyncTasks
{
    // We use a big queue to cache, to reduce add task failures.
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
        , log(DB::Logger::get("AsyncTasks"))
    {}

    struct CancelHandle;
    using CancelHandlePtr = std::shared_ptr<CancelHandle>;
    struct CancelHandle
    {
        CancelHandle() = default;
        CancelHandle(const CancelHandle &) = delete;

        bool canceled() const { return inner.load(); }

        bool blockedWaitFor(std::chrono::duration<double, std::milli> timeout)
        {
            // The task could be canceled before running.
            if (canceled())
                return true;
            std::unique_lock<std::mutex> lock(mut);
            cv.wait_for(lock, timeout, [&]() { return canceled(); });
            return canceled();
        }

        static CancelHandlePtr genAlreadyCanceled() noexcept
        {
            CancelHandlePtr h = std::make_shared<CancelHandle>();
            h->doSetCancel();
            return h;
        }

    private:
        void doSetCancel() { inner.store(true); }

        void doCancel()
        {
            // Use lock here to prevent losing signal.
            std::scoped_lock<std::mutex> lock(mut);
            inner.store(true);
            cv.notify_all();
        }

        friend struct AsyncTasks;
        std::atomic_bool inner = false;
        std::mutex mut;
        std::condition_variable cv;
    };

    struct Elem
    {
        Elem(std::future<R> && fut_, uint64_t start_ts_, std::shared_ptr<std::atomic_bool> && triggered_)
            : fut(std::move(fut_))
            , start_ts(start_ts_)
            , triggered(triggered_)
        {
            cancel = std::make_shared<CancelHandle>();
        }
        Elem(const Elem &) = delete;
        Elem(Elem &&) = default;

        std::future<R> fut;
        uint64_t start_ts;
        std::shared_ptr<CancelHandle> cancel;
        std::shared_ptr<std::atomic_bool> triggered;
    };

    // It's guarunteed a task is no longer accessible once canceled or has its result fetched.
    // NotScheduled -> InQueue, Running, NotScheduled
    // InQueue -> Running
    // Running -> Finished, NotScheduled(canceled)
    // Finished -> NotScheduled(fetched)
    enum class TaskState
    {
        NotScheduled,
        InQueue,
        Running,
        Finished,
    };

    enum class BlockCancelResult
    {
        Ok,
        NotRunning,
    };

    /// Although not mandatory, we publicize the method to allow holding the handle at the beginning of the body of async task.
    std::shared_ptr<CancelHandle> getCancelHandleFromExecutor(Key k) const
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(k);
        if unlikely (it == tasks.end())
        {
            // When the invokable is running by some executor in the thread pool,
            // it must have been registered into `tasks`.
            // So the only case that an access for a non-existing task is that the task is already cancelled asyncly.
            return CancelHandle::genAlreadyCanceled();
        }
        return it->second.cancel;
    }

    // Only unregister, no clean.
    // Use `asyncCancelTask` if there is something to clean.
    bool leakingDiscardTask(Key k)
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

    // Safety: Throws if
    // 1. The task not exist and `throw_if_noexist`.
    // 2. Throw in `result_dropper`.
    template <typename ResultDropper>
    TaskState asyncCancelTask(Key k, ResultDropper result_dropper, bool throw_if_noexist)
    {
        auto state = queryState(k);
        if (!throw_if_noexist && state == TaskState::NotScheduled)
            return state;
        if (state == TaskState::Finished)
        {
            result_dropper();
        }
        else
        {
            auto cancel_handle = getCancelHandleFromCaller(k);
            cancel_handle->doCancel();
            // Cancel logic should do clean itself
        }
        {
            std::scoped_lock l(mtx);
            auto it = tasks.find(k);
            if (it != tasks.end())
            {
                tasks.erase(it);
            }
        }

        return state;
    }

    TaskState asyncCancelTask(Key k)
    {
        return asyncCancelTask(
            k,
            []() {},
            true);
    }

    // Safety: Throws if
    // 1. The task is not found.
    // 2. The cancel_handle is already set.
    // NOTE: Consider a one producer thread one consumer thread scene, the first task is running,
    // and the second task is in queue. If we block cancel the second task here, deadlock will happen.
    // So we only allow block canceling running tasks, and users have to guaruantee all infinite loop having cancel checking.
    [[nodiscard]] BlockCancelResult blockedCancelRunningTask(Key k)
    {
        auto cancel_handle = getCancelHandleFromCaller(k);
        auto state = queryState(k);
        RUNTIME_CHECK_MSG(state != TaskState::NotScheduled, "Can't block wait a non-scheduled task");
        if (state == TaskState::InQueue)
        {
            return BlockCancelResult::NotRunning;
        }
        // Only one thread can block cancel and wait.
        RUNTIME_CHECK_MSG(!cancel_handle->canceled(), "Try block cancel running task twice");
        cancel_handle->doCancel();
        fetchResult(k);
        return BlockCancelResult::Ok;
    }

    // Safety: Throws if
    // 1. There is already a task registered with the same name and not canceled or fetched.
    bool addTask(Key k, Func f)
    {
        std::scoped_lock l(mtx);
        RUNTIME_CHECK(!tasks.contains(k));
        using P = std::packaged_task<R()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));
        std::shared_ptr<std::atomic_bool> triggered = std::make_shared<std::atomic_bool>(false);

        // The executor thread may outlive `AsyncTasks` in most cases, so we don't capture `this`.
        auto res = thread_pool->trySchedule(
            [p, triggered]() {
                triggered->store(true);
                // We can hold the cancel handle here to prevent it from destructing, but it is not necessary.
                (*p)();
                // We don't erase from `tasks` here, since we won't capture `this`
            },
            0,
            0);
        if (res)
        {
            tasks.insert({k, Elem(p->get_future(), getCurrentMillis(), std::move(triggered))});
        }
        return res;
    }

    TaskState queryState(Key key) const
    {
        using namespace std::chrono_literals;
        std::scoped_lock l(mtx);
        auto it = tasks.find(key);
        if (it == tasks.end())
            return TaskState::NotScheduled;
        if (!it->second.triggered->load())
            return TaskState::InQueue;
        if (it->second.fut.wait_for(0ms) == std::future_status::ready)
            return TaskState::Finished;
        return TaskState::Running;
    }

    bool isScheduled(Key key) const { return queryState(key) != TaskState::NotScheduled; }

    bool isInQueue(Key key) const { return queryState(key) == TaskState::InQueue; }

    bool isRunning(Key key) const { return queryState(key) == TaskState::Running; }

    bool isReady(Key key) const { return queryState(key) == TaskState::Finished; }

    uint64_t queryElapsed(Key key)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "queryElapsed meets empty key");
        return getCurrentMillis() - it->second.start_ts;
    }

    uint64_t queryStartTime(Key key, bool throw_if_not_found = true)
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        if unlikely (it == tasks.end())
        {
            if (throw_if_not_found)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "queryElapsed meets empty key");
            else
                return 0;
        }
        return it->second.start_ts;
    }

    // If a task is canceled, `fetchResult` may throw.
    R fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK_MSG(it != tasks.end(), "fetchResult meets empty key");
        std::future<R> fut = std::move(it->second.fut);
        tasks.erase(key);
        l.unlock();
        RUNTIME_CHECK_MSG(fut.valid(), "no valid future");
        return fut.get();
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

    size_t count() const { return tasks.size(); }

    static uint64_t getCurrentMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

protected:
    std::shared_ptr<CancelHandle> getCancelHandleFromCaller(Key k) const
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(k);
        RUNTIME_CHECK_MSG(it != tasks.end(), "getCancelHandleFromCaller meets empty key");
        return it->second.cancel;
    }


protected:
    std::unordered_map<Key, Elem> tasks;
    // TODO(fap) Use threadpool which supports purging from queue.
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
    LoggerPtr log;
};
} // namespace DB