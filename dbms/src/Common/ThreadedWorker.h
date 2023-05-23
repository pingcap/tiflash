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

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
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
    virtual ~ThreadedWorker()
    {
        wait();
    }

    void startInBackground() noexcept
    {
        std::call_once(start_flag, [this] {
            for (size_t index = 0; index < concurrency; ++index)
            {
                thread_manager->schedule(true, getName(), [this, index] {
                    workerLoop(index);
                });
            }
        });
    }

    void wait() noexcept
    {
        try
        {
            thread_manager->wait();
        }
        catch (...)
        {
            // This should not occur, as we should have caught all exceptions in workerLoop.
            auto error = getCurrentExceptionMessage(false);
            LOG_WARNING(log, "{} meet unexepcted error: {}", getName(), error);
        }
    }

public:
    const size_t concurrency;
    const std::shared_ptr<MPMCQueue<Src>> source_queue;
    const std::shared_ptr<MPMCQueue<Dest>> result_queue;

protected:
    const LoggerPtr log;
    std::shared_ptr<ThreadManager> thread_manager;

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
    }

    virtual String getName() const noexcept = 0;

    virtual Dest doWork(const Src & task) = 0;

    void workerLoop(size_t thread_idx)
    {
        size_t processed_tasks = 0;
        bool normal_finish = false;

        LOG_DEBUG(log, "{}#{} started", getName(), thread_idx);
        SCOPE_EXIT({
            LOG_DEBUG(log, "{}#{} finished, processed_tasks={} is_normal_finish={}", getName(), thread_idx, processed_tasks, normal_finish);
        });

        try
        {
            while (true)
            {
                Src task;
                auto pop_result = source_queue->pop(task);
                if (pop_result != MPMCQueueResult::OK)
                {
                    // When upstream is stopped (finished or cancelled), populate the stop to the the result queue.
                    if (pop_result == MPMCQueueResult::FINISHED)
                    {
                        result_queue->finish();
                        normal_finish = true;
                        break;
                    }
                    else if (pop_result == MPMCQueueResult::CANCELLED)
                    {
                        auto cancel_reason = source_queue->getCancelReason();
                        LOG_WARNING(log, "{}#{} meeting error from upstream: {}", getName(), thread_idx, cancel_reason);
                        result_queue->cancelWith(cancel_reason);
                        break;
                    }
                    else
                    {
                        RUNTIME_CHECK_MSG(false, "Unexpected pop MPMCQueueResult: {}", magic_enum::enum_name(pop_result));
                    }
                }

                auto work_result = doWork(task);
                auto push_result = result_queue->push(work_result);
                if (push_result != MPMCQueueResult::OK)
                {
                    if (push_result == MPMCQueueResult::CANCELLED)
                    {
                        // The result_queue has been closed by other workers, so we
                        // no longer process any tasks from the source.
                        break;
                    }
                    else
                    {
                        RUNTIME_CHECK_MSG(false, "Unexpected push MPMCQueueResult: {}", magic_enum::enum_name(push_result));
                    }
                }

                processed_tasks++;
            }
        }
        catch (...)
        {
            auto error = getCurrentExceptionMessage(false);
            LOG_ERROR(log, "{}#{} meet error: {}", getName(), thread_idx, error);
            result_queue->cancelWith(fmt::format("{} failed: {}", getName(), error));
        }
    }


private:
    std::once_flag start_flag;
};

} // namespace DB
