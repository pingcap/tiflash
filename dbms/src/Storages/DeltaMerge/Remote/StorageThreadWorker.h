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
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <mutex>

namespace DB::DM::Remote
{

// Similar to ThreadedWorker, but simpler.
// Allow result_queue to be nullptr if it is the last stage of pipeline.
template <typename Source, typename Result>
class StorageThreadWorker
{
public:
    StorageThreadWorker(
        const String & name_,
        const std::shared_ptr<MPMCQueue<Source>> & source_queue_,
        const std::shared_ptr<MPMCQueue<Result>> & result_queue_,
        const size_t concurrency_)
        : source_queue(source_queue_)
        , result_queue(result_queue_)
        , concurrency(concurrency_)
        , log(DB::Logger::get(name_))
        , name(name_)
        , thread_manager(newThreadManager())
    {}

    virtual ~StorageThreadWorker() = default;

    // Derived classes must wait for the thread to exit before being destructed.
    void wait()
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

    String getName() const noexcept { return name; }

    void start() noexcept
    {
        active_worker = concurrency;
        for (size_t index = 0; index < concurrency; ++index)
        {
            thread_manager->schedule(false, getName(), [this, index] {
                workerLoop(index);
                workerExist();
            });
        }
    }

    std::shared_ptr<MPMCQueue<Source>> source_queue;
    std::shared_ptr<MPMCQueue<Result>> result_queue;
    const size_t concurrency;

protected:
    virtual Result doWork(const Source & task) noexcept = 0;

    DB::LoggerPtr log;

private:
    void workerLoop(size_t thread_idx) noexcept
    {
        while (true)
        {
            Source task;
            auto pop_result = source_queue->pop(task);
            if (pop_result != MPMCQueueResult::OK)
            {
                LOG_INFO(
                    log,
                    "{}#{} pop MPMCQueueResult: {}",
                    getName(),
                    thread_idx,
                    magic_enum::enum_name(pop_result));
                break;
            }
            auto task_result = doWork(task);
            if (result_queue != nullptr && task_result != nullptr)
            {
                auto res = result_queue->push(task_result);
                if (res != MPMCQueueResult::OK)
                {
                    LOG_INFO(log, "{}#{} push MPMCQueueResult: {}", getName(), thread_idx, magic_enum::enum_name(res));
                    break;
                }
            }
        }
    }

    void workerExist()
    {
        if (--active_worker <= 0)
        {
            if (result_queue != nullptr)
            {
                if (source_queue->getStatus() == MPMCQueueStatus::FINISHED)
                {
                    result_queue->finish();
                }
                else
                {
                    result_queue->cancel();
                }
            }
        }
    }

private:
    String name;
    std::shared_ptr<ThreadManager> thread_manager;
    std::atomic<Int64> active_worker{0};
};

} // namespace DB::DM::Remote
