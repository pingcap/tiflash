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

#include <Flash/Executor/PipelineExecutorStatus.h>

namespace DB
{
ExecutionResult PipelineExecutorStatus::toExecutionResult() noexcept
{
    std::lock_guard lock(mu);
    return exception_ptr
        ? ExecutionResult::fail(exception_ptr)
        : ExecutionResult::success();
}

std::exception_ptr PipelineExecutorStatus::getExceptionPtr() noexcept
{
    std::lock_guard lock(mu);
    return exception_ptr;
}

String PipelineExecutorStatus::getExceptionMsg() noexcept
{
    try
    {
        auto cur_exception_ptr = getExceptionPtr();
        if (!cur_exception_ptr)
            return "";
        std::rethrow_exception(cur_exception_ptr);
    }
    catch (const DB::Exception & e)
    {
        return e.message();
    }
    catch (...)
    {
        return getCurrentExceptionMessage(false, true);
    }
}

void PipelineExecutorStatus::onErrorOccurred(const String & err_msg) noexcept
{
    DB::Exception e(err_msg);
    onErrorOccurred(std::make_exception_ptr(e));
}

bool PipelineExecutorStatus::setExceptionPtr(const std::exception_ptr & exception_ptr_) noexcept
{
    RUNTIME_ASSERT(exception_ptr_ != nullptr);
    std::lock_guard lock(mu);
    if (exception_ptr != nullptr)
        return false;
    exception_ptr = exception_ptr_;
    return true;
}

bool PipelineExecutorStatus::isWaitMode() noexcept
{
    return !result_queue.has_value();
}

void PipelineExecutorStatus::onErrorOccurred(const std::exception_ptr & exception_ptr_) noexcept
{
    if (setExceptionPtr(exception_ptr_))
    {
        cancel();
        LOG_WARNING(log, "error occured and cancel the query");
    }
}

void PipelineExecutorStatus::wait() noexcept
{
    {
        std::unique_lock lock(mu);
        RUNTIME_ASSERT(isWaitMode());
        cv.wait(lock, [&] { return 0 == active_event_count; });
    }
    LOG_DEBUG(log, "query finished and wait done");
}

ResultQueuePtr PipelineExecutorStatus::getConsumedResultQueue() noexcept
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!isWaitMode());
    auto consumed_result_queue = *result_queue;
    assert(consumed_result_queue);
    return consumed_result_queue;
}

void PipelineExecutorStatus::consume(ResultHandler & result_handler) noexcept
{
    RUNTIME_ASSERT(result_handler);
    auto consumed_result_queue = getConsumedResultQueue();
    Block ret;
    while (consumed_result_queue->pop(ret) == MPMCQueueResult::OK)
        result_handler(ret);
    // In order to ensure that `onEventFinish` has finished calling at this point
    // and avoid referencing the already destructed `mu` in `onEventFinish`.
    {
        std::lock_guard lock(mu);
        RUNTIME_ASSERT(0 == active_event_count);
    }
    LOG_DEBUG(log, "query finished and consume done");
}

void PipelineExecutorStatus::onEventSchedule() noexcept
{
    std::lock_guard lock(mu);
    ++active_event_count;
}

void PipelineExecutorStatus::onEventFinish() noexcept
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(active_event_count > 0);
    --active_event_count;
    if (0 == active_event_count)
    {
        // It is not expected for a query to be finished more than one time.
        RUNTIME_ASSERT(!is_finished);
        is_finished = true;

        if (isWaitMode())
        {
            cv.notify_all();
        }
        else
        {
            assert(*result_queue);
            (*result_queue)->finish();
        }
    }
}

void PipelineExecutorStatus::cancel() noexcept
{
    is_cancelled.store(true, std::memory_order_release);
}

ResultQueuePtr PipelineExecutorStatus::toConsumeMode(size_t queue_size) noexcept
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!result_queue.has_value());
    result_queue.emplace(std::make_shared<ResultQueue>(queue_size));
    return *result_queue;
}
} // namespace DB
