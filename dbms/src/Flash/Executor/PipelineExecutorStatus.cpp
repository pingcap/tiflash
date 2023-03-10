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
#include <assert.h>

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
    assert(exception_ptr_ != nullptr);
    std::lock_guard lock(mu);
    if (exception_ptr != nullptr)
        return false;
    exception_ptr = exception_ptr_;
    return true;
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
        cv.wait(lock, [&] { return 0 == active_event_count; });
    }
    LOG_DEBUG(log, "query finished and wait done");
}

void PipelineExecutorStatus::onEventSchedule() noexcept
{
    std::lock_guard lock(mu);
    ++active_event_count;
}

void PipelineExecutorStatus::onEventFinish() noexcept
{
    std::lock_guard lock(mu);
    assert(active_event_count > 0);
    --active_event_count;
    if (0 == active_event_count)
        cv.notify_all();
}

void PipelineExecutorStatus::cancel() noexcept
{
    is_cancelled.store(true, std::memory_order_release);
}
} // namespace DB
