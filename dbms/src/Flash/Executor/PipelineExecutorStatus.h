// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Executor/ExecutionResult.h>
#include <Flash/Executor/ResultQueue.h>

#include <atomic>
#include <exception>
#include <mutex>

namespace DB
{
class PipelineExecutorStatus : private boost::noncopyable
{
public:
    static constexpr auto timeout_err_msg = "error with timeout";

    PipelineExecutorStatus()
        : log(Logger::get())
    {}

    explicit PipelineExecutorStatus(const String & req_id)
        : log(Logger::get(req_id))
    {}

    ExecutionResult toExecutionResult() noexcept;

    std::exception_ptr getExceptionPtr() noexcept;
    String getExceptionMsg() noexcept;

    void onEventSchedule() noexcept;

    void onEventFinish() noexcept;

    void onErrorOccurred(const String & err_msg) noexcept;
    void onErrorOccurred(const std::exception_ptr & exception_ptr_) noexcept;

    void wait() noexcept;

    template <typename Duration>
    void waitFor(const Duration & timeout_duration)
    {
        bool is_timeout = false;
        {
            std::unique_lock lock(mu);
            is_timeout = !cv.wait_for(lock, timeout_duration, [&] { return 0 == active_event_count; });
        }
        if (is_timeout)
        {
            LOG_WARNING(log, "wait timeout");
            onErrorOccurred(timeout_err_msg);
            throw Exception(timeout_err_msg);
        }
        LOG_DEBUG(log, "query finished and wait done");
    }

    void cancel() noexcept;

    ALWAYS_INLINE bool isCancelled() noexcept
    {
        return is_cancelled.load(std::memory_order_acquire);
    }

    ResultQueuePtr registerResultQueue(size_t queue_size) noexcept;

private:
    bool setExceptionPtr(const std::exception_ptr & exception_ptr_) noexcept;

private:
    LoggerPtr log;

    std::mutex mu;
    std::condition_variable cv;
    std::exception_ptr exception_ptr;
    UInt32 active_event_count{0};

    std::atomic_bool is_cancelled{false};

    bool is_finished{false};

    // `result_queue.finish` can only be called in `onEventFinish` because `result_queue.pop` cannot end until events end.
    // `registerResultQueue` is called before event scheduled, so is safe to use result_queue without lock.
    // If `registerResultQueue` is called, `result_queue` must be safely visible in `onEventFinish`.
    std::optional<ResultQueuePtr> result_queue;
};
} // namespace DB
