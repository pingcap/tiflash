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

#include <Common/CPUAffinityManager.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/MinTSOScheduler.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

#include <chrono>
#include <ext/scope_guard.h>
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
extern const char exception_during_mpp_write_err_to_tunnel[];
extern const char force_no_local_region_for_mpp_task[];
extern const char random_task_lifecycle_failpoint[];
} // namespace FailPoints

MPPTask::MPPTask(const mpp::TaskMeta & meta_, const ContextPtr & context_)
    : context(context_)
    , meta(meta_)
    , id(meta.start_ts(), meta.task_id())
    , log(Logger::get("MPPTask", id.toString()))
    , mpp_task_statistics(id, meta.address())
    , needed_threads(0)
    , schedule_state(ScheduleState::WAITING)
{}

MPPTask::~MPPTask()
{
    /// MPPTask maybe destructed by different thread, set the query memory_tracker
    /// to current_memory_tracker in the destructor
    if (current_memory_tracker != memory_tracker)
        current_memory_tracker = memory_tracker;
    closeAllTunnels("");
    if (schedule_state == ScheduleState::SCHEDULED)
    {
        /// the threads of this task are not fully freed now, since the BlockIO and DAGContext are not destructed
        /// TODO: finish all threads before here, except the current one.
        manager->releaseThreadsFromScheduler(needed_threads);
        schedule_state = ScheduleState::COMPLETED;
    }
    LOG_FMT_DEBUG(log, "finish MPPTask: {}", id.toString());
}

void MPPTask::abortTunnels(const String & message, AbortType abort_type)
{
    if (abort_type == AbortType::ONCANCELLATION)
    {
        closeAllTunnels(message);
    }
    else
    {
        RUNTIME_ASSERT(tunnel_set != nullptr, log, "mpp task without tunnel set");
        tunnel_set->writeError(message);
    }
}

void MPPTask::abortReceivers()
{
    if (likely(receiver_set != nullptr))
    {
        receiver_set->cancel();
    }
}

void MPPTask::abortDataStreams(AbortType abort_type)
{
    /// When abort type is ONERROR, it means MPPTask already known it meet error, so let the remaining task stop silently to avoid too many useless error message
    bool is_kill = abort_type == AbortType::ONCANCELLATION;
    context->getProcessList().sendCancelToQuery(context->getCurrentQueryId(), context->getClientInfo().current_user, is_kill);
}

void MPPTask::closeAllTunnels(const String & reason)
{
    if (likely(tunnel_set))
        tunnel_set->close(reason);
}

void MPPTask::finishWrite()
{
    RUNTIME_ASSERT(tunnel_set != nullptr, log, "mpp task without tunnel set");
    tunnel_set->finishWrite();
}

void MPPTask::run()
{
    newThreadManager()->scheduleThenDetach(true, "MPPTask", [self = shared_from_this()] { self->runImpl(); });
}

void MPPTask::registerTunnels(const mpp::DispatchTaskRequest & task_request)
{
    tunnel_set = std::make_shared<MPPTunnelSet>(log->identifier());
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
        LOG_FMT_DEBUG(log, "begin to register the tunnel {}", tunnel->id());
        if (status != INITIALIZING)
            throw Exception(fmt::format("The tunnel {} can not be registered, because the task is not in initializing state", tunnel->id()));
        tunnel_set->registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
        if (!dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
        }
    }
}

void MPPTask::initExchangeReceivers()
{
    receiver_set = std::make_shared<MPPReceiverSet>(log->identifier());
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
                executor_id);
            if (status != RUNNING)
                throw Exception("exchange receiver map can not be initialized, because the task is not in running state");

            receiver_set->addExchangeReceiver(executor_id, exchange_receiver);
            new_thread_count_of_exchange_receiver += exchange_receiver->computeNewThreadCount();
        }
        return true;
    });
    dag_context->setMPPReceiverSet(receiver_set);
}

std::pair<MPPTunnelPtr, String> MPPTask::getTunnel(const ::mpp::EstablishMPPConnectionRequest * request)
{
    if (status == CANCELLED)
    {
        auto err_msg = fmt::format(
            "can't find tunnel ({} + {}) because the task is cancelled",
            request->sender_meta().task_id(),
            request->receiver_meta().task_id());
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

void MPPTask::unregisterTask()
{
    if (manager != nullptr)
    {
        LOG_DEBUG(log, "task unregistered");
        manager->unregisterTask(this);
    }
    else
    {
        LOG_ERROR(log, "task manager is unset");
    }
}

void MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    dag_req = getDAGRequestFromStringWithRetry(task_request.encoded_plan());
    TMTContext & tmt_context = context->getTMTContext();
    /// MPP task will only use key ranges in mpp::DispatchTaskRequest::regions/mpp::DispatchTaskRequest::table_regions.
    /// The ones defined in tipb::TableScan will never be used and can be removed later.
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(task_request.regions(), task_request.table_regions(), tmt_context);
    LOG_FMT_DEBUG(
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

    dag_context->tunnel_set = tunnel_set;
    // register task.
    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_FMT_DEBUG(log, "begin to register the task {}", id.toString());

    if (dag_context->isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_non_root_mpp_task);
    }
    if (!task_manager->registerTask(shared_from_this()))
    {
        throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": Failed to register MPP Task", Errors::Coprocessor::BadRequest);
    }

    mpp_task_statistics.initializeExecutorDAG(dag_context.get());
    mpp_task_statistics.logTracingJson();
}

void MPPTask::preprocess()
{
    auto start_time = Clock::now();
    initExchangeReceivers();
    DAGQuerySource dag(*context);
    executeQuery(dag, *context, false, QueryProcessingStage::Complete);
    auto end_time = Clock::now();
    dag_context->compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    mpp_task_statistics.setCompileTimestamp(start_time, end_time);
    mpp_task_statistics.recordReadWaitIndex(*dag_context);
}

void MPPTask::runImpl()
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();
    if (!switchStatus(INITIALIZING, RUNNING))
    {
        LOG_WARNING(log, "task not in initializing state, skip running");
        return;
    }
    Stopwatch stopwatch;
    GET_METRIC(tiflash_coprocessor_request_count, type_run_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_run_mpp_task).Observe(stopwatch.elapsedSeconds());
    });
    String err_msg;
    try
    {
        LOG_FMT_INFO(log, "task starts preprocessing");
        preprocess();
        needed_threads = estimateCountOfNewThreads();
        LOG_FMT_DEBUG(log, "Estimate new thread count of query :{} including tunnel_threads: {} , receiver_threads: {}", needed_threads, dag_context->tunnel_set->getRemoteTunnelCnt(), new_thread_count_of_exchange_receiver);

        scheduleOrWait();

        LOG_FMT_INFO(log, "task starts running");
        memory_tracker = current_memory_tracker;
        if (status.load() != RUNNING)
        {
            /// when task is in running state, canceling the task will call sendCancelToQuery to do the cancellation, however
            /// if the task is cancelled during preprocess, sendCancelToQuery may just be ignored because the processlist of
            /// current task is not registered yet, so need to check the task status explicitly
            throw Exception("task not in running state, may be cancelled");
        }
        mpp_task_statistics.start();
        auto from = dag_context->getBlockIO().in;
        from->readPrefix();
        LOG_DEBUG(log, "begin read ");

        while (from->read())
            continue;

        from->readSuffix();
        finishWrite();

        const auto & return_statistics = mpp_task_statistics.collectRuntimeStatistics();
        LOG_FMT_DEBUG(
            log,
            "finish write with {} rows, {} blocks, {} bytes",
            return_statistics.rows,
            return_statistics.blocks,
            return_statistics.bytes);
    }
    catch (...)
    {
        err_msg = getCurrentExceptionMessage(true, true);
    }

    if (err_msg.empty())
    {
        if (switchStatus(RUNNING, FINISHED))
            LOG_INFO(log, "finish task");
        else
            LOG_FMT_WARNING(log, "finish task which is in {} state", taskStatusToString(status));
        if (status == FINISHED)
        {
            // todo when error happens, should try to update the metrics if it is available
            auto throughput = dag_context->getTableScanThroughput();
            if (throughput.first)
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
            LOG_FMT_ERROR(log, "task running meets error: {}", err_msg);
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
    LOG_FMT_INFO(log, "task ends, time cost is {} ms.", stopwatch.elapsedMilliseconds());
    // unregister flag is only for FailPoint usage, to produce the situation that MPPTask is destructed
    // by grpc CancelMPPTask thread;
    bool unregister = true;
    fiu_do_on(FailPoints::random_task_lifecycle_failpoint, {
        if (!err_msg.empty())
            unregister = false;
    });
    if (unregister)
        unregisterTask();

    mpp_task_statistics.end(status.load(), err_string);
    mpp_task_statistics.logTracingJson();
}

void MPPTask::handleError(const String & error_msg)
{
    if (manager == nullptr || !manager->isTaskToBeCancelled(id))
        abort(error_msg, AbortType::ONERROR);
}

void MPPTask::abort(const String & message, AbortType abort_type)
{
    String abort_type_string;
    TaskStatus next_task_status;
    switch (abort_type)
    {
    case AbortType::ONCANCELLATION:
        abort_type_string = "ONCANCELLATION";
        next_task_status = CANCELLED;
        break;
    case AbortType::ONERROR:
        abort_type_string = "ONERROR";
        next_task_status = FAILED;
        break;
    }
    LOG_FMT_WARNING(log, "Begin abort task: {}, abort type: {}", id.toString(), abort_type_string);
    while (true)
    {
        auto previous_status = status.load();
        if (previous_status == FINISHED || previous_status == CANCELLED || previous_status == FAILED)
        {
            LOG_FMT_WARNING(log, "task already in {} state", taskStatusToString(previous_status));
            return;
        }
        else if (previous_status == INITIALIZING && switchStatus(INITIALIZING, next_task_status))
        {
            err_string = message;
            /// if the task is in initializing state, mpp task can return error to TiDB directly,
            /// so just close all tunnels here
            closeAllTunnels(message);
            unregisterTask();
            LOG_WARNING(log, "Finish abort task from uninitialized");
            return;
        }
        else if (previous_status == RUNNING && switchStatus(RUNNING, next_task_status))
        {
            /// abort the components from top to bottom because if bottom components are aborted
            /// first, the top components may see an error caused by the abort, which is not
            /// the original error
            err_string = message;
            abortTunnels(message, abort_type);
            abortDataStreams(abort_type);
            abortReceivers();
            scheduleThisTask(ScheduleState::FAILED);
            /// runImpl is running, leave remaining work to runImpl
            LOG_WARNING(log, "Finish abort task from running");
            return;
        }
    }
}

void MPPTask::cancel(const String & reason)
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();
    abort(reason, AbortType::ONCANCELLATION);
}

bool MPPTask::switchStatus(TaskStatus from, TaskStatus to)
{
    return status.compare_exchange_strong(from, to);
}

void MPPTask::scheduleOrWait()
{
    if (!manager->tryToScheduleTask(shared_from_this()))
    {
        LOG_FMT_INFO(log, "task waits for schedule");
        Stopwatch stopwatch;
        double time_cost = 0;
        {
            std::unique_lock lock(schedule_mu);
            schedule_cv.wait(lock, [&] { return schedule_state != ScheduleState::WAITING; });
            time_cost = stopwatch.elapsedSeconds();
            GET_METRIC(tiflash_task_scheduler_waiting_duration_seconds).Observe(time_cost);

            if (schedule_state == ScheduleState::EXCEEDED)
            {
                throw Exception(fmt::format("{} is failed to schedule because of exceeding the thread hard limit in min-tso scheduler after waiting for {}s.", id.toString(), time_cost));
            }
            else if (schedule_state == ScheduleState::FAILED)
            {
                throw Exception(fmt::format("{} is failed to schedule because of being cancelled in min-tso scheduler after waiting for {}s.", id.toString(), time_cost));
            }
        }
        LOG_FMT_INFO(log, "task waits for {} s to schedule and starts to run in parallel.", time_cost);
    }
}

void MPPTask::scheduleThisTask(ScheduleState state)
{
    std::unique_lock lock(schedule_mu);
    if (schedule_state == ScheduleState::WAITING)
    {
        LOG_FMT_INFO(log, "task is {}.", state == ScheduleState::SCHEDULED ? "scheduled" : " failed to schedule");
        schedule_state = state;
        schedule_cv.notify_one();
    }
}

int MPPTask::estimateCountOfNewThreads()
{
    if (dag_context == nullptr || dag_context->getBlockIO().in == nullptr || dag_context->tunnel_set == nullptr)
        throw Exception("It should not estimate the threads for the uninitialized task" + id.toString());

    // Estimated count of new threads from InputStreams(including ExchangeReceiver), remote MppTunnels s.
    return dag_context->getBlockIO().in->estimateNewThreadCount() + 1
        + dag_context->tunnel_set->getRemoteTunnelCnt();
}

int MPPTask::getNeededThreads()
{
    if (needed_threads == 0)
    {
        throw Exception(" the needed_threads of task " + id.toString() + " is not initialized!");
    }
    return needed_threads;
}

bool MPPTask::isScheduled()
{
    std::unique_lock lock(schedule_mu);
    return schedule_state == ScheduleState::SCHEDULED;
}

} // namespace DB
