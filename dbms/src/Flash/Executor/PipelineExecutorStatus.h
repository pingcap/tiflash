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

#include <Flash/Executor/ExecutionResult.h>

#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>

namespace DB
{
class PipelineExecutorStatus : private boost::noncopyable
{
public:
    static constexpr auto timeout_err_msg = "error with timeout";

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
            is_timeout = !cv.wait_for(lock, timeout_duration, [&] { return 0 == active_event_count; });
        }
        if (is_timeout)
        {
            onErrorOccurred(timeout_err_msg);
            throw Exception(timeout_err_msg);
        }
    }

    void cancel();

    bool isCancelled()
    {
        return is_cancelled.load(std::memory_order_acquire);
    }

private:
    std::mutex mu;
    std::condition_variable cv;
    std::exception_ptr exception_ptr;
    UInt32 active_event_count{0};

    std::atomic_bool is_cancelled{false};
};
} // namespace DB
