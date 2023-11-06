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

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <mutex>

namespace DB
{

/// A ThreadedWorker spawns n (n=concurrency) threads to process tasks.
/// Each thread takes a task from the source queue, do some work, and put the result into the
/// result queue.
/// ThreadedWorker manages the state of its result queue, for example, when all tasks are finished
/// or when there are errors, the result queue will be finished or cancelled.
template <typename Src, typename Dest>
class ThreadedWorker
{
public:
    /// WARNING: As Base class destructors always run AFTER the derived class destructors,
    /// derived workers must implement its own destructors, like:
    ///
    /// ```c++
    /// ~MyWorker() override { wait(); }
    /// ```
    ///
    /// Otherwise, the `doWork()` may be still running and accessing derived class
    /// members, while derived class is already destructed.
    virtual ~ThreadedWorker() = default;

    void startInBackground() noexcept
    {
        std::call_once(start_flag, [this] {
            LOG_DEBUG(log, "Starting {} workers, concurrency={}", getName(), concurrency);
            active_workers = concurrency;

            watch_start.restart();
            for (size_t index = 0; index < concurrency; ++index)
            {
                thread_manager->schedule(true, getName(), [this, index] {
                    total_wait_schedule_ms += watch_start.elapsedMilliseconds();

                    workerLoop(index);
                    handleWorkerFinished();
                });
            }
        });
    }

    void wait() noexcept
    {
        std::call_once(wait_flag, [this] {
            try
            {
                // thread_manager->wait can be only called once.
                thread_manager->wait();
            }
            catch (...)
            {
                // This should not occur, as we should have caught all exceptions in workerLoop.
                auto error = getCurrentExceptionMessage(false);
                LOG_WARNING(log, "{} meet unexepcted error: {}", getName(), error);
            }
        });
    }

    ThreadedWorker(
        std::shared_ptr<MPMCQueue<Src>> source_queue_,
        std::shared_ptr<MPMCQueue<Dest>> result_queue_,
        LoggerPtr log_,
        size_t concurrency_)
        : concurrency(concurrency_)
        , source_queue(source_queue_)
        , result_queue(result_queue_)
        , log(log_)
        , thread_manager(newThreadManager())
    {
        RUNTIME_CHECK(concurrency > 0, concurrency);
    }

public:
    const size_t concurrency;
    const std::shared_ptr<MPMCQueue<Src>> source_queue;
    const std::shared_ptr<MPMCQueue<Dest>> result_queue;

protected:
    const LoggerPtr log;
    std::shared_ptr<ThreadManager> thread_manager;
    std::atomic<Int64> active_workers = 0;

    virtual String getName() const noexcept = 0;

    virtual Dest doWork(const Src & task) = 0;

private:
    void handleWorkerFinished() noexcept
    {
        active_workers--;
        if (active_workers == 0)
        {
            std::call_once(finish_flag, [this] {
                LOG_DEBUG(
                    log,
                    "{} workers finished, total_processed_tasks={} concurrency={} elapsed={:.3f}s "
                    "total_wait_schedule={:.3f}s total_wait_upstream={:.3f}s total_wait_downstream={:.3f}s",
                    getName(),
                    total_processed_tasks,
                    concurrency,
                    watch_start.elapsedSeconds(),
                    total_wait_schedule_ms / 1000.0,
                    total_wait_upstream_ms / 1000.0,
                    total_wait_downstream_ms / 1000.0);
                // Note: the result queue may be already cancelled, but it is fine.
                result_queue->finish();
            });
        }
    }

    void workerLoop(size_t thread_idx) noexcept
    {
        try
        {
            while (true)
            {
                Src task;

                Stopwatch w{CLOCK_MONOTONIC_COARSE};
                auto pop_result = source_queue->pop(task);
                total_wait_upstream_ms += w.elapsedMilliseconds();

                if (pop_result != MPMCQueueResult::OK)
                {
                    if (pop_result == MPMCQueueResult::FINISHED)
                    {
                        // No more work to do, just exit.
                        // The FINISH signal will be passed to downstreams when
                        // all workers are exited.
                        break;
                    }
                    else if (pop_result == MPMCQueueResult::CANCELLED)
                    {
                        // There are errors from upstream, populate the error to downstreams
                        // immediately.
                        auto cancel_reason = source_queue->getCancelReason();
                        LOG_WARNING(log, "{}#{} meeting error from upstream: {}", getName(), thread_idx, cancel_reason);
                        result_queue->cancelWith(cancel_reason);
                        break;
                    }
                    else
                    {
                        RUNTIME_CHECK_MSG(
                            false,
                            "Unexpected pop MPMCQueueResult: {}",
                            magic_enum::enum_name(pop_result));
                    }
                }

                auto work_result = doWork(task);

                w.restart();
                auto push_result = result_queue->push(work_result);
                total_wait_downstream_ms += w.elapsedMilliseconds();

                if (push_result != MPMCQueueResult::OK)
                {
                    if (push_result == MPMCQueueResult::CANCELLED)
                    {
                        // There are two possible cases:
                        // Case A: The upstream is cancelled and one of the worker cancelled the downstream.
                        // Case B: There is something wrong with the downstream
                        // In case B, we need to populate the error to upstream, so that the whole
                        // pipeline is cancelled.
                        auto cancel_reason = result_queue->getCancelReason();
                        LOG_WARNING(
                            log,
                            "{}#{} meeting error from downstream: {}",
                            getName(),
                            thread_idx,
                            cancel_reason);
                        source_queue->cancelWith(cancel_reason);
                        break;
                    }
                    else
                    {
                        RUNTIME_CHECK_MSG(
                            false,
                            "Unexpected push MPMCQueueResult: {}",
                            magic_enum::enum_name(push_result));
                    }
                }

                total_processed_tasks++;
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "{}#{} meet error: {}", getName(), thread_idx, getCurrentExceptionMessage(true));
            auto cancel_reason = fmt::format("{} failed: {}", getName(), getCurrentExceptionMessage(false));
            source_queue->cancelWith(cancel_reason);
            result_queue->cancelWith(cancel_reason);
        }
    }

private:
    Stopwatch watch_start{CLOCK_MONOTONIC_COARSE};
    std::once_flag start_flag;
    std::once_flag wait_flag;
    std::once_flag finish_flag;
    std::atomic<Int64> total_processed_tasks = 0;
    std::atomic<Int64> total_wait_schedule_ms = 0;
    std::atomic<Int64> total_wait_upstream_ms = 0;
    std::atomic<Int64> total_wait_downstream_ms = 0;
};

} // namespace DB
