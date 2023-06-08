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
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Executor/ResultQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskProfileInfo.h>

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

    ExecutionResult toExecutionResult();

    std::exception_ptr getExceptionPtr();
    String getExceptionMsg();

    void onEventSchedule();

    void onEventFinish();

    void onErrorOccurred(const String & err_msg);
    void onErrorOccurred(const std::exception_ptr & exception_ptr_);

    void wait();

    template <typename Duration>
    void waitFor(const Duration & timeout_duration)
    {
        bool is_timeout = false;
        {
            std::unique_lock lock(mu);
            RUNTIME_ASSERT(isWaitMode());
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

    void consume(ResultHandler & result_handler);

    template <typename Duration>
    void consumeFor(ResultHandler & result_handler, const Duration & timeout_duration)
    {
        RUNTIME_ASSERT(result_handler);
        auto consumed_result_queue = getConsumedResultQueue();
        bool is_timeout = false;
        try
        {
            Block ret;
            while (true)
            {
                auto res = consumed_result_queue->popTimeout(ret, timeout_duration);
                if (res == MPMCQueueResult::TIMEOUT)
                {
                    is_timeout = true;
                    break;
                }
                else if (res == MPMCQueueResult::OK)
                {
                    result_handler(ret);
                }
                else
                {
                    break;
                }
            }
        }
        catch (...)
        {
            // If result_handler throws an error, here should notify the query to terminate, and wait for the end of the query.
            onErrorOccurred(std::current_exception());
        }
        if (is_timeout)
        {
            LOG_WARNING(log, "wait timeout");
            onErrorOccurred(timeout_err_msg);
            throw Exception(timeout_err_msg);
        }
        else
        {
            // In order to ensure that `onEventFinish` has finished calling at this point
            // and avoid referencing the already destructed `mu` in `onEventFinish`.
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return 0 == active_event_count; });
        }
        LOG_DEBUG(log, "query finished and consume done");
    }

    void cancel();

    ALWAYS_INLINE bool isCancelled()
    {
        return is_cancelled.load(std::memory_order_acquire);
    }

    ResultQueuePtr toConsumeMode(size_t queue_size);

    void update(const TaskProfileInfo & task_profile_info)
    {
        query_profile_info.merge(task_profile_info);
    }

    const QueryProfileInfo & getQueryProfileInfo() const
    {
        return query_profile_info;
    }

private:
    bool setExceptionPtr(const std::exception_ptr & exception_ptr_);

    // Need to be called under lock.
    bool isWaitMode();

    ResultQueuePtr getConsumedResultQueue();

private:
    LoggerPtr log;

    std::mutex mu;
    std::condition_variable cv;
    std::exception_ptr exception_ptr;
    UInt32 active_event_count{0};

    std::atomic_bool is_cancelled{false};

    bool is_finished{false};

    // `result_queue.finish` can only be called in `onEventFinish` because `result_queue.pop` cannot end until events end.
    std::optional<ResultQueuePtr> result_queue;

    QueryProfileInfo query_profile_info;
};
} // namespace DB
