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
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/UniThreadPool.h>

#include <future>
#include <magic_enum.hpp>
#include <unordered_map>

namespace DB
{

namespace AsyncTaskHelper
{
// It's guaranteed a task is no longer accessible once canceled or has its result fetched.
// NotScheduled -> InQueue, Running, NotScheduled
// InQueue -> Running
// Running -> Finished, NotScheduled(canceled)
// Finished -> NotScheduled(fetched)
// NOTE: magic_enum can't work properly with enums in template classes due to compiler opts.
enum class TaskState
{
    NotScheduled,
    InQueue,
    Running,
    Finished,
};
} // namespace AsyncTaskHelper

struct GeneralCancelHandle
{
    using GeneralCancelHandlePtr = std::shared_ptr<GeneralCancelHandle>;
    GeneralCancelHandle() = default;
    GeneralCancelHandle(const GeneralCancelHandle &) = delete;

    bool isCanceled() const { return inner.load(); }

    bool blockedWaitFor(std::chrono::duration<double, std::milli> timeout)
    {
        // The task could be canceled before running.
        if (isCanceled())
            return true;
        std::unique_lock<std::mutex> lock(mut);
        cv.wait_for(lock, timeout, [&]() { return isCanceled(); });
        return isCanceled();
    }

    static GeneralCancelHandlePtr genAlreadyCanceled() noexcept
    {
        auto h = std::make_shared<GeneralCancelHandle>();
        h->inner.store(true);
        return h;
    }

    static GeneralCancelHandlePtr genNotCanceled() noexcept
    {
        auto h = std::make_shared<GeneralCancelHandle>();
        h->inner.store(false);
        return h;
    }

    // Make this private to forbid called by multiple consumers.
    void doCancel()
    {
        // Use lock here to prevent losing signal.
        std::scoped_lock lock(mut);
        inner.store(true);
        cv.notify_all();
    }

private:
    std::atomic_bool inner = false;
    std::mutex mut;
    std::condition_variable cv;
};

// Key should support `fmt::formatter`.
template <typename Key, typename Func, typename R>
struct AsyncTasks
{
    // We use a big queue to cache, to reduce add task failures.
    explicit AsyncTasks(uint64_t pool_size, uint64_t free_pool_size, uint64_t queue_size)
        : thread_pool(std::make_unique<ThreadPool>(pool_size, free_pool_size, queue_size))
        , log(DB::Logger::get())
    {}

    void shutdown()
    {
        LOG_INFO(log, "Pending {} tasks when destructing", count());
        // To avoid the "last owner" problem in worker thread.
        thread_pool->wait();
        shut.store(true);
        LOG_INFO(log, "Finish finalize thread pool");
    }

    ~AsyncTasks()
    {
        if (!shut.load())
        {
            LOG_INFO(log, "Destruct without shutdown");
            // Potential deadlock if the instance is held and released directly or indirectly by a task in its worker.
            shutdown();
        }
    }

    using TaskState = AsyncTaskHelper::TaskState;

    using CancelHandle = GeneralCancelHandle;
    using CancelHandlePtr = std::shared_ptr<CancelHandle>;

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
    /// Usage:
    /// 1. If the task is in `Finished` state
    ///     It's result will be cleaned with `result_dropper`.
    /// 2. If the task is in `Running` state
    ///     Make sure the executor will do the clean.
    /// 3. If the tasks is in `InQueue` state
    ///     The task will directly return when it's eventually run by a thread.
    /// 4. If the tasks is in `NotScheduled` state
    ///     `throw_if_noexist` controls whether to throw.
    /// NOTE: The task element will be removed after calling this function.
    template <typename ResultDropper>
    TaskState asyncCancelTask(Key k, ResultDropper result_dropper, bool throw_if_noexist)
    {
        auto cancel_handle = getCancelHandleFromCaller(k, throw_if_noexist);
        if (cancel_handle)
        {
            cancel_handle->doCancel();
            // Cancel logic should do clean itself
        }

        auto state = queryState(k);
        if (!throw_if_noexist && state == TaskState::NotScheduled)
            return state;
        if (state == TaskState::Finished)
        {
            result_dropper();
        }

        // `result_dropper` may remove the task by `fetchResult`.
        leakingDiscardTask(k);

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
    // 1. The task is not found, and throw_on_no_exist.
    // 2. The cancel_handle is already set.
    /// Usage:
    /// 1. If the task is in `Finished`/`Running` state
    ///     It's result is returned. The Caller may do the rest cleaning.
    /// 2. If the tasks is in `InQueue` state
    ///     The task will directly return when it's eventually run by a thread.
    /// 3. If the tasks is in `NotScheduled` state
    ///     It will throw on `throw_on_no_exist`.
    /// Returns:
    /// 1. The TaskState before the task is canceled
    /// 2. Exception
    /// NOTE: The task element will be removed after calling this function.
    [[nodiscard]] TaskState blockedCancelRunningTask(Key k, bool throw_on_no_exist = true)
    {
        auto cancel_handle = getCancelHandleFromCaller(k);
        auto state = queryState(k);
        if (state == TaskState::NotScheduled)
        {
            if (throw_on_no_exist)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't block wait a non-scheduled task");
            }
            else
            {
                return state;
            }
        }

        // Only one thread can block cancel and wait.
        RUNTIME_CHECK_MSG(!cancel_handle->isCanceled(), "Try block cancel running task twice");
        cancel_handle->doCancel();
        if (state == TaskState::InQueue)
        {
            leakingDiscardTask(k);
            return state;
        }
        // Consider a one producer thread one consumer thread scene, the first task is running,
        // and the second task is in queue. If we block cancel the second task here, deadlock will happen.
        // So we only block on fetching running tasks, and users have to guaruantee cancel checking.
        fetchResult(k);
        return state;
    }

    using CancelFunc = std::function<void()>;
    // Safety: Throws if
    // 1. There is already a task registered with the same name and not canceled or fetched.
    bool addTaskWithCancel(Key k, Func f, CancelFunc cf)
    {
        if (shut.load())
            return false;
        std::scoped_lock l(mtx);
        RUNTIME_CHECK(!tasks.contains(k));
        using P = std::packaged_task<R()>;
        std::shared_ptr<P> p = std::make_shared<P>(P(f));
        std::shared_ptr<std::atomic_bool> triggered = std::make_shared<std::atomic_bool>(false);
        auto trigger_cloned = triggered;
        auto elem = Elem(p->get_future(), getCurrentMillis(), std::move(trigger_cloned));
        auto cancel_handle = elem.cancel;

        auto running_mut = std::make_shared<std::mutex>();
        // Task could not run unless registered in `tasks`.
        std::scoped_lock caller_running_lock(*running_mut);
        // The executor thread may outlive `AsyncTasks` in most cases, so we don't capture `this`.
        auto job = [p, triggered, running_mut, cancel_handle, cf]() {
            RUNTIME_CHECK(triggered != nullptr);
            RUNTIME_CHECK(running_mut != nullptr);
            RUNTIME_CHECK(cancel_handle != nullptr);
            RUNTIME_CHECK(p != nullptr);
            if (cancel_handle->isCanceled())
            {
                cf();
                return;
            }
            {
                // (*p)() will not be reordered before this lock.
                std::scoped_lock worker_running_lock(*running_mut);
                triggered->store(true);
            }
            // We can hold the cancel handle here to prevent it from destructing, but it is not necessary.
            (*p)();
            // We don't erase from `tasks` here, since we won't capture `this`
        };
        auto res = thread_pool->trySchedule(job, 0, 0);
        SYNC_FOR("after_AsyncTasks::addTask_scheduled");
        if (res)
        {
            tasks.insert({k, std::move(elem)});
        }
        SYNC_FOR("before_AsyncTasks::addTask_quit");
        return res;
    }

    bool addTask(Key k, Func f)
    {
        return addTaskWithCancel(k, f, []() {});
    }

    TaskState unsafeQueryState(Key key) const
    {
        using namespace std::chrono_literals;
        auto it = tasks.find(key);
        if (it == tasks.end())
            return TaskState::NotScheduled;
        if (!it->second.triggered->load())
            return TaskState::InQueue;
        if (it->second.fut.wait_for(0ms) == std::future_status::ready)
            return TaskState::Finished;
        return TaskState::Running;
    }

    TaskState queryState(Key key) const
    {
        std::scoped_lock l(mtx);
        return unsafeQueryState(key);
    }

    bool isScheduled(Key key) const { return queryState(key) != TaskState::NotScheduled; }

    bool isInQueue(Key key) const { return queryState(key) == TaskState::InQueue; }

    bool isRunning(Key key) const { return queryState(key) == TaskState::Running; }

    bool isReady(Key key) const { return queryState(key) == TaskState::Finished; }

    std::optional<uint64_t> queryElapsed(Key key) const
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        if unlikely (it == tasks.end())
        {
            return std::nullopt;
        }
        return getCurrentMillis() - it->second.start_ts;
    }

    std::optional<uint64_t> queryStartTime(Key key) const
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        if unlikely (it == tasks.end())
        {
            return std::nullopt;
        }
        return it->second.start_ts;
    }

    // If a task is canceled, `fetchResult` may throw.
    R fetchResult(Key key)
    {
        std::unique_lock<std::mutex> l(mtx);
        auto it = tasks.find(key);
        RUNTIME_CHECK(it != tasks.end(), key);
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
        RUNTIME_CHECK(it != tasks.end(), key);
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
    std::shared_ptr<CancelHandle> getCancelHandleFromCaller(Key k, bool throw_if_noexist = true) const
    {
        std::scoped_lock<std::mutex> l(mtx);
        auto it = tasks.find(k);
        if (it == tasks.end())
        {
            if (throw_if_noexist)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "getCancelHandleFromCaller can't find key");
            }
            else
            {
                return nullptr;
            }
        }
        return it->second.cancel;
    }

protected:
    std::unordered_map<Key, Elem> tasks;
    // TODO(fap) Use threadpool which supports purging from queue.
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
    LoggerPtr log;
    std::atomic_bool shut = false;
};

} // namespace DB
