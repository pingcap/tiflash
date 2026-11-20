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
#include <Flash/Executor/ResultQueue.h>
#include <Flash/Mpp/MPPReceiverSet.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/OneTimeNotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Operators/CTE.h>
#include <Operators/SharedQueue.h>

#include <exception>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>

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

String PipelineExecutorContext::getTrimmedErrMsg()
{
    try
    {
        auto cur_exception_ptr = getExceptionPtr();
        if (!cur_exception_ptr)
            return "";
        std::rethrow_exception(cur_exception_ptr);
    }
    catch (...)
    {
        auto err_msg = getCurrentExceptionMessage(true, true);
        if (likely(!err_msg.empty()))
            trimStackTrace(err_msg);
        return err_msg;
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
        waitForDrain(lock);
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
    waitForDrain(lock);
    LOG_DEBUG(log, "query finished and consume done");
}

void PipelineExecutorContext::incActiveRefCount()
{
    std::lock_guard lock(mu);
    ++active_ref_count;
}

void PipelineExecutorContext::registerTask(const Task * task)
{
    std::lock_guard lock(mu);
    active_tasks.emplace(task, "non-pipeline task");
    ++active_ref_count;
}

void PipelineExecutorContext::unregisterTask(const Task * task)
{
    {
        std::lock_guard lock(mu);
        active_tasks.erase(task);
    }
    decActiveRefCount();
}

void PipelineExecutorContext::describeTask(const Task * task, String operators)
{
    std::lock_guard lock(mu);
    active_tasks.at(task) = std::move(operators);
}

String PipelineExecutorContext::dumpActiveTasksLocked(size_t limit)
{
    String dump
        = fmt::format("query_id={} active_ref_count={} tasks={}", query_id, active_ref_count, active_tasks.size());
    size_t count = 0;
    for (const auto & [task, operators] : active_tasks)
    {
        if (count++ == limit)
        {
            dump += " (remaining tasks omitted)";
            break;
        }
        dump += fmt::format(
            " [task={} status={} thread_id={} operators={}]",
            static_cast<const void *>(task),
            magic_enum::enum_name(task->getDiagnosticStatus()),
            task->getDiagnosticThreadId(),
            operators);
    }
    return dump;
}

String PipelineExecutorContext::dumpActiveTasks()
{
    std::lock_guard lock(mu);
    return dumpActiveTasksLocked(std::numeric_limits<size_t>::max());
}

void PipelineExecutorContext::waitForDrain(std::unique_lock<std::mutex> & lock)
{
    cv.wait(lock, [&] { return active_ref_count == 0 || cancelled_at != std::chrono::steady_clock::time_point{}; });
    auto next_warning = cancelled_at + std::chrono::seconds(30);
    while (active_ref_count != 0)
    {
        if (cv.wait_until(lock, next_warning, [&] { return active_ref_count == 0; }))
            break;
        const auto elapsed
            = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - cancelled_at).count();
        try
        {
            auto dump = dumpActiveTasksLocked(32);
            lock.unlock();
            SCOPE_EXIT({ lock.lock(); });
            LOG_WARNING(
                log,
                "Cancelled executor has not drained for {}s: {}. Worker references must remain alive.",
                elapsed,
                dump);
        }
        catch (...)
        {
            // Diagnostics may fail under memory pressure. Never leave wait()
            // while workers can still access the executor or query context.
        }
        next_warning = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    }
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
        std::vector<std::function<void()>> callbacks;
        {
            std::lock_guard lock(mu);
            cancelled_at = std::chrono::steady_clock::now();
            callbacks.swap(cancellation_callbacks);
            cv.notify_all();
        }
        for (auto & callback : callbacks)
            callback();
        cancelSharedQueues();
        cancelOneTimeFutures();
        if (likely(dag_context))
        {
            // Cancel the tunnel_set and mpp_receiver_set here to prevent
            // pipeline tasks waiting in the WAIT_FOR_NOTIFY state from never being notified.
            if (dag_context->tunnel_set)
                dag_context->tunnel_set->close(getTrimmedErrMsg(), false);

            auto cte = dag_context->getCTESink();
            if (cte)
                cte->notifyCancel<false>(getTrimmedErrMsg());

            for (auto & p : dag_context->getCTESource())
                p.second->notifyCancel<false>(getTrimmedErrMsg());

            if (auto mpp_receiver_set = dag_context->getMPPReceiverSet(); mpp_receiver_set)
                mpp_receiver_set->cancel();
        }
        cancelResultQueueIfNeed();
        if likely (TaskScheduler::instance && !query_id.empty())
            TaskScheduler::instance->cancel(TaskCancelInfo{
                .query_id = query_id,
                .keyspace_id = getKeyspaceID(),
                .resource_group_name = resource_group_name});
    }
}

void PipelineExecutorContext::addCancellationCallback(std::function<void()> callback)
{
    {
        std::lock_guard lock(mu);
        if (!isCancelled())
        {
            cancellation_callbacks.push_back(std::move(callback));
            return;
        }
    }
    callback();
}

ResultQueuePtr PipelineExecutorContext::toConsumeMode(size_t queue_size)
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!result_queue.has_value());
    RUNTIME_CHECK_MSG(!isCancelled(), "query has been cancelled.");
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

void PipelineExecutorContext::addOneTimeFuture(const OneTimeNotifyFuturePtr & future)
{
    std::lock_guard lock(mu);
    RUNTIME_CHECK_MSG(!isCancelled(), "query has been cancelled.");
    assert(future);
    one_time_futures.push_back(future);
}

void PipelineExecutorContext::cancelOneTimeFutures()
{
    std::vector<OneTimeNotifyFuturePtr> tmp;
    {
        std::lock_guard lock(mu);
        std::swap(tmp, one_time_futures);
    }
    for (const auto & future : tmp)
        future->finish();
}

void PipelineExecutorContext::cancelResultQueueIfNeed()
{
    ResultQueue * tmp{nullptr};
    {
        std::lock_guard lock(mu);
        if (isWaitMode())
            return;
        tmp = (*result_queue).get();
    }
    assert(tmp);
    tmp->cancel();
}
} // namespace DB
