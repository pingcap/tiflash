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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Operators/SharedQueue.h>

#include <exception>

namespace DB
{
ExecutionResult PipelineExecutorContext::toExecutionResult()
{
    std::lock_guard lock(mu);
    return exception_ptr ? ExecutionResult::fail(exception_ptr) : ExecutionResult::success();
}

std::exception_ptr PipelineExecutorContext::getExceptionPtr()
{
    std::lock_guard lock(mu);
    return exception_ptr;
}

String PipelineExecutorContext::getExceptionMsg()
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

void PipelineExecutorContext::onErrorOccurred(const String & err_msg)
{
    DB::Exception e(err_msg);
    onErrorOccurred(std::make_exception_ptr(e));
}

bool PipelineExecutorContext::setExceptionPtr(const std::exception_ptr & exception_ptr_)
{
    RUNTIME_ASSERT(exception_ptr_ != nullptr);
    std::lock_guard lock(mu);
    if (exception_ptr != nullptr)
        return false;
    exception_ptr = exception_ptr_;
    return true;
}

bool PipelineExecutorContext::isWaitMode()
{
    return !result_queue.has_value();
}

void PipelineExecutorContext::onErrorOccurred(const std::exception_ptr & exception_ptr_)
{
    if (setExceptionPtr(exception_ptr_))
    {
        cancel();
        LOG_WARNING(log, "error {} occured and cancel the query", getExceptionMsg());
    }
}

void PipelineExecutorContext::wait()
{
    {
        std::unique_lock lock(mu);
        RUNTIME_ASSERT(isWaitMode());
        cv.wait(lock, [&] { return 0 == active_ref_count; });
    }
    LOG_DEBUG(log, "query finished and wait done");
}

ResultQueuePtr PipelineExecutorContext::getConsumedResultQueue()
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!isWaitMode());
    auto consumed_result_queue = *result_queue;
    assert(consumed_result_queue);
    return consumed_result_queue;
}

void PipelineExecutorContext::consume(ResultHandler & result_handler)
{
    RUNTIME_ASSERT(result_handler);
    auto consumed_result_queue = getConsumedResultQueue();
    try
    {
        Block ret;
        while (consumed_result_queue->pop(ret) == MPMCQueueResult::OK)
            result_handler(ret);
    }
    catch (...)
    {
        // If result_handler throws an error, here should notify the query to terminate, and wait for the end of the query.
        onErrorOccurred(std::current_exception());
    }
    // In order to ensure that `decActiveRefCount` has finished calling at this point
    // and avoid referencing the already destructed `mu` in `decActiveRefCount`.
    std::unique_lock lock(mu);
    cv.wait(lock, [&] { return 0 == active_ref_count; });
    LOG_DEBUG(log, "query finished and consume done");
}

void PipelineExecutorContext::incActiveRefCount()
{
    std::lock_guard lock(mu);
    ++active_ref_count;
}

void PipelineExecutorContext::decActiveRefCount()
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(active_ref_count > 0);
    --active_ref_count;
    if (0 == active_ref_count)
    {
        // It is not expected for a query to be finished more than one time.
        RUNTIME_ASSERT(!is_finished);
        is_finished = true;

        if (!isWaitMode())
        {
            assert(*result_queue);
            (*result_queue)->finish();
        }
        cv.notify_all();
    }
}

void PipelineExecutorContext::cancel()
{
    bool origin_value = false;
    if (is_cancelled.compare_exchange_strong(origin_value, true, std::memory_order_release))
    {
        if (likely(dag_context))
        {
            // Cancel the tunnel_set here to prevent pipeline tasks waiting in the WAIT_FOR_NOTIFY state from never being notified.
            if (dag_context->tunnel_set)
                dag_context->tunnel_set->close("", false);
        }
        cancelSharedQueues();
        if likely (TaskScheduler::instance && !query_id.empty())
            TaskScheduler::instance->cancel(query_id, resource_group_name);
    }
}

ResultQueuePtr PipelineExecutorContext::toConsumeMode(size_t queue_size)
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!result_queue.has_value());
    result_queue.emplace(std::make_shared<ResultQueue>(queue_size));
    return *result_queue;
}

void PipelineExecutorContext::addSharedQueue(const SharedQueuePtr & shared_queue)
{
    std::lock_guard lock(mu);
    RUNTIME_CHECK_MSG(!isCancelled(), "query has been cancelled.");
    assert(shared_queue);
    shared_queues.push_back(shared_queue);
}

void PipelineExecutorContext::cancelSharedQueues()
{
    std::vector<SharedQueuePtr> tmp;
    {
        std::lock_guard lock(mu);
        std::swap(tmp, shared_queues);
    }
    for (const auto & shared_queue : tmp)
        shared_queue->cancel();
}
} // namespace DB
