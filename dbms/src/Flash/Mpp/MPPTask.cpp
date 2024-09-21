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

#include <Common/CPUAffinityManager.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExecutionSummaryCollector.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Flash/executeQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

#include <chrono>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <map>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_mpp_register_non_root_mpp_task[];
extern const char exception_before_mpp_register_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_root_mpp_task[];
extern const char exception_during_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char force_no_local_region_for_mpp_task[];
} // namespace FailPoints

MPPTask::MPPTask(const mpp::TaskMeta & meta_, const ContextPtr & context_)
    : meta(meta_)
    , id(meta.start_ts(), meta.task_id())
    , context(context_)
    , manager(context_->getTMTContext().getMPPTaskManager().get())
    , schedule_entry(manager, id)
    , log(Logger::get(id.toString()))
    , mpp_task_statistics(id, meta.address())
{
    current_memory_tracker = nullptr;
}

MPPTask::~MPPTask()
{
    /// MPPTask maybe destructed by different thread, set the query memory_tracker
    /// to current_memory_tracker in the destructor
    if (process_list_entry != nullptr && current_memory_tracker != process_list_entry->get().getMemoryTrackerPtr().get())
        current_memory_tracker = process_list_entry->get().getMemoryTrackerPtr().get();
    abortTunnels("", true);
    LOG_INFO(log, "finish MPPTask: {}", id.toString());
}

void MPPTask::abortTunnels(const String & message, bool wait_sender_finish)
{
    {
        std::unique_lock lock(mtx);
        if (unlikely(tunnel_set == nullptr))
            return;
    }
    tunnel_set->close(message, wait_sender_finish);
}

void MPPTask::abortReceivers()
{
    {
        std::unique_lock lock(mtx);
        if unlikely (receiver_set == nullptr)
            return;
    }
    receiver_set->cancel();
}

void MPPTask::abortDataStreams(AbortType abort_type)
{
    /// When abort type is ONERROR, it means MPPTask already known it meet error, so let the remaining task stop silently to avoid too many useless error message
    bool is_kill = abort_type == AbortType::ONCANCELLATION;
    if (auto query_executor = query_executor_holder.tryGet(); query_executor)
    {
        assert(query_executor.value());
        (*query_executor)->cancel(is_kill);
    }
}

void MPPTask::finishWrite()
{
    RUNTIME_ASSERT(tunnel_set != nullptr, log, "mpp task without tunnel set");
    if (dag_context->collect_execution_summaries)
    {
        ExecutionSummaryCollector summary_collector(*dag_context);
        tunnel_set->sendExecutionSummary(summary_collector.genExecutionSummaryResponse());
    }
    tunnel_set->finishWrite();
}

void MPPTask::run()
{
    newThreadManager()->scheduleThenDetach(true, "MPPTask", [self = shared_from_this()] { self->runImpl(); });
}

void MPPTask::registerTunnels(const mpp::DispatchTaskRequest & task_request)
{
    auto tunnel_set_local = std::make_shared<MPPTunnelSet>(log->identifier());
    std::chrono::seconds timeout(task_request.timeout());
    const auto & exchange_sender = dag_req.root_executor().exchange_sender();

    for (int i = 0; i < exchange_sender.encoded_task_meta_size(); ++i)
    {
        // exchange sender will register the tunnels and wait receiver to found a connection.
        mpp::TaskMeta task_meta;
        if (unlikely(!task_meta.ParseFromString(exchange_sender.encoded_task_meta(i))))
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        bool is_local = context->getSettingsRef().enable_local_tunnel && meta.address() == task_meta.address();
        bool is_async = !is_local && context->getSettingsRef().enable_async_server;
        MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>(task_meta, task_request.meta(), timeout, context->getSettingsRef().max_threads, is_local, is_async, log->identifier());
        LOG_DEBUG(log, "begin to register the tunnel {}, is_local: {}, is_async: {}", tunnel->id(), is_local, is_async);
        if (status != INITIALIZING)
            throw Exception(fmt::format("The tunnel {} can not be registered, because the task is not in initializing state", tunnel->id()));
        tunnel_set_local->registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
        if (!dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
        }
    }
    {
        std::unique_lock lock(mtx);
        if (status != INITIALIZING)
            throw Exception(fmt::format("The tunnels can not be registered, because the task is not in initializing state"));
        tunnel_set = std::move(tunnel_set_local);
    }
    dag_context->tunnel_set = tunnel_set;
}

void MPPTask::initExchangeReceivers()
{
    auto receiver_set_local = std::make_shared<MPPReceiverSet>(log->identifier());
    traverseExecutors(&dag_req, [&](const tipb::Executor & executor) {
        if (executor.tp() == tipb::ExecType::TypeExchangeReceiver)
        {
            assert(executor.has_executor_id());
            const auto & executor_id = executor.executor_id();
            // In order to distinguish different exchange receivers.
            auto exchange_receiver = std::make_shared<ExchangeReceiver>(
                std::make_shared<GRPCReceiverContext>(
                    executor.exchange_receiver(),
                    dag_context->getMPPTaskMeta(),
                    context->getTMTContext().getKVCluster(),
                    context->getTMTContext().getMPPTaskManager(),
                    context->getSettingsRef().enable_local_tunnel,
                    context->getSettingsRef().enable_async_grpc_client),
                executor.exchange_receiver().encoded_task_meta_size(),
                context->getMaxStreams(),
                log->identifier(),
                executor_id,
                executor.fine_grained_shuffle_stream_count());
            if (status != RUNNING)
                throw Exception("exchange receiver map can not be initialized, because the task is not in running state");

            receiver_set_local->addExchangeReceiver(executor_id, exchange_receiver);
        }
        return true;
    });
    {
        std::unique_lock lock(mtx);
        if (status != RUNNING)
            throw Exception("exchange receiver map can not be initialized, because the task is not in running state");
        receiver_set = std::move(receiver_set_local);
    }
    dag_context->setMPPReceiverSet(receiver_set);
}

std::pair<MPPTunnelPtr, String> MPPTask::getTunnel(const ::mpp::EstablishMPPConnectionRequest * request)
{
    if (status == CANCELLED || status == FAILED)
    {
        auto err_msg = fmt::format(
            "can't find tunnel ({} + {}) because the task is aborted, error message = {}",
            request->sender_meta().task_id(),
            request->receiver_meta().task_id(),
            getErrString());
        return {nullptr, err_msg};
    }

    MPPTaskId receiver_id{request->receiver_meta().start_ts(), request->receiver_meta().task_id()};
    RUNTIME_ASSERT(tunnel_set != nullptr, log, "mpp task without tunnel set");
    auto tunnel_ptr = tunnel_set->getTunnelByReceiverTaskId(receiver_id);
    if (tunnel_ptr == nullptr)
    {
        auto err_msg = fmt::format(
            "can't find tunnel ({} + {})",
            request->sender_meta().task_id(),
            request->receiver_meta().task_id());
        return {nullptr, err_msg};
    }
    return {tunnel_ptr, ""};
}

String MPPTask::getErrString() const
{
    std::lock_guard lock(mtx);
    return err_string;
}

void MPPTask::setErrString(const String & message)
{
    std::lock_guard lock(mtx);
    err_string = message;
}

void MPPTask::unregisterTask()
{
    auto [result, reason] = manager->unregisterTask(id);
    if (result)
        LOG_DEBUG(log, "task unregistered");
    else
        LOG_WARNING(log, "task failed to unregister, reason: {}", reason);
}

void MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    dag_req = getDAGRequestFromStringWithRetry(task_request.encoded_plan());
    TMTContext & tmt_context = context->getTMTContext();
    /// MPP task will only use key ranges in mpp::DispatchTaskRequest::regions/mpp::DispatchTaskRequest::table_regions.
    /// The ones defined in tipb::TableScan will never be used and can be removed later.
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(task_request.regions(), task_request.table_regions(), tmt_context);
    LOG_DEBUG(
        log,
        "Handling {} regions from {} physical tables in MPP task",
        tables_regions_info.regionCount(),
        tables_regions_info.tableCount());

    // set schema ver and start ts.
    auto schema_ver = task_request.schema_ver();
    auto start_ts = task_request.meta().start_ts();

    context->setSetting("read_tso", start_ts);
    context->setSetting("schema_version", schema_ver);
    if (unlikely(task_request.timeout() < 0))
    {
        /// this is only for test
        context->setSetting("mpp_task_timeout", static_cast<Int64>(5));
        context->setSetting("mpp_task_running_timeout", static_cast<Int64>(10));
    }
    else
    {
        context->setSetting("mpp_task_timeout", task_request.timeout());
        if (task_request.timeout() > 0)
        {
            /// in the implementation, mpp_task_timeout is actually the task writing tunnel timeout
            /// so make the mpp_task_running_timeout a little bigger than mpp_task_timeout
            context->setSetting("mpp_task_running_timeout", task_request.timeout() + 30);
        }
    }
    context->getTimezoneInfo().resetByDAGRequest(dag_req);

    bool is_root_mpp_task = false;
    const auto & exchange_sender = dag_req.root_executor().exchange_sender();
    if (exchange_sender.encoded_task_meta_size() == 1)
    {
        /// root mpp task always has 1 task_meta because there is only one TiDB
        /// node for each mpp query
        mpp::TaskMeta task_meta;
        if (!task_meta.ParseFromString(exchange_sender.encoded_task_meta(0)))
        {
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        }
        is_root_mpp_task = task_meta.task_id() == -1;
    }
    dag_context = std::make_unique<DAGContext>(dag_req, task_request.meta(), is_root_mpp_task);
    dag_context->log = log;
    dag_context->tables_regions_info = std::move(tables_regions_info);
    dag_context->tidb_host = context->getClientInfo().current_address.toString();

    context->setDAGContext(dag_context.get());
    process_list_entry = setProcessListElement(*context, dag_context->dummy_query_string, dag_context->dummy_ast.get());
    dag_context->setProcessListEntry(process_list_entry);

    if (dag_context->isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_non_root_mpp_task);
    }

    // register tunnels
    registerTunnels(task_request);

    // register task.
    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_DEBUG(log, "begin to register the task {}", id.toString());

    if (dag_context->isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_non_root_mpp_task);
    }
    auto [result, reason] = task_manager->registerTask(shared_from_this());
    if (!result)
    {
        throw TiFlashException(fmt::format("Failed to register MPP Task {}, reason: {}", id.toString(), reason), Errors::Coprocessor::BadRequest);
    }

    mpp_task_statistics.initializeExecutorDAG(dag_context.get());
    mpp_task_statistics.logTracingJson();
}

void MPPTask::preprocess()
{
    auto start_time = Clock::now();
    initExchangeReceivers();
    LOG_DEBUG(log, "init exchange receiver done");
    query_executor_holder.set(queryExecute(*context));
    LOG_DEBUG(log, "init query executor done");
    {
        std::unique_lock lock(mtx);
        if (status != RUNNING)
            throw Exception("task not in running state, may be cancelled");
        for (auto & r : dag_context->getCoprocessorReaders())
            receiver_set->addCoprocessorReader(r);
        new_thread_count_of_mpp_receiver += receiver_set->getExternalThreadCnt();
    }
    auto end_time = Clock::now();
    dag_context->compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    mpp_task_statistics.setCompileTimestamp(start_time, end_time);
    mpp_task_statistics.recordReadWaitIndex(*dag_context);
}

void MPPTask::runImpl()
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();
    RUNTIME_ASSERT(current_memory_tracker == process_list_entry->get().getMemoryTrackerPtr().get(), log, "The current memory tracker is not set correctly for MPPTask::runImpl");
    if (!switchStatus(INITIALIZING, RUNNING))
    {
        LOG_WARNING(log, "task not in initializing state, skip running");
        unregisterTask();
        return;
    }
    Stopwatch stopwatch;
    GET_METRIC(tiflash_coprocessor_request_count, type_run_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_run_mpp_task).Observe(stopwatch.elapsedSeconds());
    });
<<<<<<< HEAD
=======

    // set cancellation hook
    context->setCancellationHook([this] { return is_cancelled.load(); });

>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))
    String err_msg;
    try
    {
        LOG_DEBUG(log, "task starts preprocessing");
        preprocess();
        auto time_cost_in_preprocess_ms = stopwatch.elapsedMilliseconds();
        LOG_DEBUG(log, "task preprocess done");
        schedule_entry.setNeededThreads(estimateCountOfNewThreads());
        LOG_DEBUG(log, "Estimate new thread count of query: {} including tunnel_threads: {}, receiver_threads: {}", schedule_entry.getNeededThreads(), dag_context->tunnel_set->getExternalThreadCnt(), new_thread_count_of_mpp_receiver);

        scheduleOrWait();

        auto time_cost_in_schedule_ms = stopwatch.elapsedMilliseconds() - time_cost_in_preprocess_ms;
        LOG_INFO(log, "task starts running, time cost in schedule: {} ms, time cost in preprocess {} ms", time_cost_in_schedule_ms, time_cost_in_preprocess_ms);
        if (status.load() != RUNNING)
        {
            /// when task is in running state, canceling the task will call sendCancelToQuery to do the cancellation, however
            /// if the task is cancelled during preprocess, sendCancelToQuery may just be ignored because the processlist of
            /// current task is not registered yet, so need to check the task status explicitly
            throw Exception("task not in running state, may be cancelled");
        }
        mpp_task_statistics.start();

        auto result = query_executor_holder->execute();
        auto log_level = Poco::Message::PRIO_DEBUG;
        if (!result.is_success || status != RUNNING)
            log_level = Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(log, log_level, "mpp task finish execute, success: {}, status: {}", result.is_success, magic_enum::enum_name(status.load()));
        if (likely(result.is_success))
        {
            // finish receiver
            receiver_set->close();
            // finish MPPTunnel
            finishWrite();
        }
        else
        {
            err_msg = result.err_msg;
        }

        const auto & return_statistics = mpp_task_statistics.collectRuntimeStatistics();
        LOG_DEBUG(
            log,
            "finish with {} rows, {} blocks, {} bytes",
            return_statistics.rows,
            return_statistics.blocks,
            return_statistics.bytes);
    }
    catch (...)
    {
        auto catch_err_msg = getCurrentExceptionMessage(true, true);
        err_msg = err_msg.empty() ? catch_err_msg : fmt::format("{}, {}", err_msg, catch_err_msg);
    }

    if (err_msg.empty())
    {
        if (switchStatus(RUNNING, FINISHED))
            LOG_DEBUG(log, "finish task");
        else
            LOG_WARNING(log, "finish task which is in {} state", magic_enum::enum_name(status.load()));
        if (status == FINISHED)
        {
            // todo when error happens, should try to update the metrics if it is available
            if (auto throughput = dag_context->getTableScanThroughput(); throughput.first)
                GET_METRIC(tiflash_storage_logical_throughput_bytes).Observe(throughput.second);
            auto process_info = context->getProcessListElement()->getInfo();
            auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
            GET_METRIC(tiflash_coprocessor_request_memory_usage, type_run_mpp_task).Observe(peak_memory);
            mpp_task_statistics.setMemoryPeak(peak_memory);
        }
    }
    else
    {
        if (status == RUNNING)
        {
            LOG_ERROR(log, "task running meets error: {}", err_msg);
            /// trim the stack trace to avoid too many useless information in log
            trimStackTrace(err_msg);
            try
            {
                handleError(err_msg);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Meet error while try to handle error in MPPTask");
            }
        }
    }
    mpp_task_statistics.end(status.load(), getErrString());
    mpp_task_statistics.logTracingJson();

    LOG_DEBUG(log, "task ends, time cost is {} ms.", stopwatch.elapsedMilliseconds());
    unregisterTask();
}

void MPPTask::handleError(const String & error_msg)
{
    auto updated_msg = fmt::format("From {}: {}", id.toString(), error_msg);
    manager->abortMPPQuery(id.start_ts, updated_msg, AbortType::ONERROR);
    if (!registered)
        // if the task is not registered, need to cancel it explicitly
        abort(error_msg, AbortType::ONERROR);
}

void MPPTask::abort(const String & message, AbortType abort_type)
{
    auto abort_type_string = magic_enum::enum_name(abort_type);
    TaskStatus next_task_status;
    switch (abort_type)
    {
    case AbortType::ONCANCELLATION:
        next_task_status = CANCELLED;
        break;
    case AbortType::ONERROR:
        next_task_status = FAILED;
        break;
    }
    LOG_WARNING(log, "Begin abort task: {}, abort type: {}", id.toString(), abort_type_string);
    while (true)
    {
        auto previous_status = status.load();
        if (previous_status == FINISHED || previous_status == CANCELLED || previous_status == FAILED)
        {
            LOG_WARNING(log, "task already in {} state", magic_enum::enum_name(previous_status));
            break;
        }
        else if (previous_status == INITIALIZING && switchStatus(INITIALIZING, next_task_status))
        {
            setErrString(message);
            /// if the task is in initializing state, mpp task can return error to TiDB directly,
            /// so just close all tunnels here
            abortTunnels("", false);
            LOG_WARNING(log, "Finish abort task from uninitialized");
            break;
        }
        else if (previous_status == RUNNING && switchStatus(RUNNING, next_task_status))
        {
            /// abort the components from top to bottom because if bottom components are aborted
            /// first, the top components may see an error caused by the abort, which is not
            /// the original error
            setErrString(message);
            abortTunnels(message, false);
            abortDataStreams(abort_type);
            abortReceivers();
            scheduleThisTask(ScheduleState::FAILED);
            /// runImpl is running, leave remaining work to runImpl
            LOG_WARNING(log, "Finish abort task from running");
            break;
        }
    }
    is_cancelled = true;
}

bool MPPTask::switchStatus(TaskStatus from, TaskStatus to)
{
    return status.compare_exchange_strong(from, to);
}

void MPPTask::scheduleOrWait()
{
    if (!manager->tryToScheduleTask(schedule_entry))
    {
        schedule_entry.waitForSchedule();
    }
}

bool MPPTask::scheduleThisTask(ScheduleState state)
{
    return schedule_entry.schedule(state);
}

int MPPTask::estimateCountOfNewThreads()
{
    auto query_executor = query_executor_holder.tryGet();
    RUNTIME_CHECK_MSG(
        query_executor && dag_context->tunnel_set != nullptr,
        "It should not estimate the threads for the uninitialized task {}",
        id.toString());

    // Estimated count of new threads from query executor, MppTunnels, mpp_receivers.
    assert(query_executor.value());
    return (*query_executor)->estimateNewThreadCount() + 1
        + dag_context->tunnel_set->getExternalThreadCnt()
        + new_thread_count_of_mpp_receiver;
}

} // namespace DB
