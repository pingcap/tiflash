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
#include <Common/ThresholdUtils.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <fmt/core.h>

#include <chrono>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <map>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_before_mpp_make_non_root_mpp_task_active[];
extern const char exception_before_mpp_make_root_mpp_task_active[];
extern const char exception_before_mpp_register_non_root_mpp_task[];
extern const char exception_before_mpp_register_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_root_mpp_task[];
extern const char exception_during_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char force_no_local_region_for_mpp_task[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
} // namespace FailPoints


namespace
{
void injectFailPointBeforeRegisterTunnel(bool is_root_task)
{
    if (is_root_task)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_non_root_mpp_task);
    }
}

void injectFailPointBeforeMakeMPPTaskPublic(bool is_root_task)
{
    if (is_root_task)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_make_root_mpp_task_active);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_make_non_root_mpp_task_active);
    }
}

void injectFailPointBeforeRegisterMPPTask(bool is_root_task)
{
    if (is_root_task)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_non_root_mpp_task);
    }
}

void injectFailPointDuringRegisterTunnel(bool is_root_task)
{
    if (!is_root_task)
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
}
} // namespace

void MPPTaskMonitorHelper::initAndAddself(MPPTaskManager * manager_, const String & task_unique_id_)
{
    manager = manager_;
    task_unique_id = task_unique_id_;
    added_to_monitor = manager->addMonitoredTask(task_unique_id);
}

MPPTaskMonitorHelper::~MPPTaskMonitorHelper()
{
    if (added_to_monitor)
    {
        manager->removeMonitoredTask(task_unique_id);
    }
}

MPPTask::MPPTask(const mpp::TaskMeta & meta_, const ContextPtr & context_)
    : meta(meta_)
    , id(meta)
    , context(context_)
    , manager(context_->getTMTContext().getMPPTaskManager().get())
    , schedule_entry(manager, id)
    , log(Logger::get(id.toString()))
    , mpp_task_statistics(id, meta.address())
{
    assert(manager != nullptr);
    current_memory_tracker = nullptr;
    mpp_task_monitor_helper.initAndAddself(manager, id.toString());
}

void MPPTask::initForTest()
{
    dag_context = std::make_unique<DAGContext>(100);
    context->setDAGContext(dag_context.get());
    schedule_entry.setNeededThreads(10);
}

MPPTask::~MPPTask()
{
    /// MPPTask maybe destructed by different thread, set the query memory_tracker
    /// to current_memory_tracker in the destructor
    auto * query_memory_tracker = getMemoryTracker();
    if (query_memory_tracker != nullptr && current_memory_tracker != query_memory_tracker)
        current_memory_tracker = query_memory_tracker;
    abortTunnels("", true);
    LOG_INFO(log, "finish MPPTask: {}", id.toString());
}

bool MPPTask::isRootMPPTask() const
{
    return dag_context->isRootMPPTask();
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

void MPPTask::abortQueryExecutor()
{
    if (auto query_executor = query_executor_holder.tryGet(); query_executor)
    {
        assert(query_executor.value());
        (*query_executor)->cancel();
    }
}

void MPPTask::finishWrite()
{
    RUNTIME_ASSERT(tunnel_set != nullptr, log, "mpp task without tunnel set");
    if (dag_context->collect_execution_summaries
        && !ReportExecutionSummaryToCoordinator(meta.mpp_version(), meta.report_execution_summary()))
        tunnel_set->sendExecutionSummary(mpp_task_statistics.genExecutionSummaryResponse());
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
    const auto & exchange_sender = dag_context->dag_request.rootExecutor().exchange_sender();
    size_t tunnel_queue_memory_bound = getAverageThreshold(
        context->getSettingsRef().max_buffered_bytes_in_executor,
        exchange_sender.encoded_task_meta_size());
    CapacityLimits queue_limit(
        std::max(5, context->getSettingsRef().max_threads * 5),
        tunnel_queue_memory_bound); // MPMCQueue can benefit from a slightly larger queue size

    for (int i = 0; i < exchange_sender.encoded_task_meta_size(); ++i)
    {
        // exchange sender will register the tunnels and wait receiver to found a connection.
        mpp::TaskMeta task_meta;
        if (unlikely(!task_meta.ParseFromString(exchange_sender.encoded_task_meta(i))))
            throw TiFlashException(
                "Failed to decode task meta info in ExchangeSender",
                Errors::Coprocessor::BadRequest);

        /// when the receiver task is root task, it should never be local tunnel
        bool is_local = context->getSettingsRef().enable_local_tunnel && task_meta.task_id() != -1
            && meta.address() == task_meta.address();
        bool is_async = !is_local && context->getSettingsRef().enable_async_server;
        MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>(
            task_meta,
            task_request.meta(),
            timeout,
            queue_limit,
            is_local,
            is_async,
            log->identifier());

        LOG_DEBUG(log, "begin to register the tunnel {}, is_local: {}, is_async: {}", tunnel->id(), is_local, is_async);

        if (status != INITIALIZING)
            throw Exception(fmt::format(
                "The tunnel {} can not be registered, because the task is not in initializing state",
                tunnel->id()));

        MPPTaskId task_id(task_meta);
        RUNTIME_CHECK_MSG(
            id.gather_id.gather_id == task_id.gather_id.gather_id,
            "MPP query has different gather id, should be something wrong in TiDB side");
        tunnel_set_local->registerTunnel(task_id, tunnel);
        injectFailPointDuringRegisterTunnel(dag_context->isRootMPPTask());
    }
    {
        std::unique_lock lock(mtx);
        if (status != INITIALIZING)
            throw Exception(
                fmt::format("The tunnels can not be registered, because the task is not in initializing state"));
        tunnel_set = std::move(tunnel_set_local);
    }
    dag_context->tunnel_set = tunnel_set;
}

void MPPTask::initExchangeReceivers()
{
    auto receiver_set_local = std::make_shared<MPPReceiverSet>(log->identifier());
    try
    {
        dag_context->dag_request.traverse([&](const tipb::Executor & executor) {
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
                    executor.fine_grained_shuffle_stream_count(),
                    context->getSettingsRef());

                receiver_set_local->addExchangeReceiver(executor_id, exchange_receiver);

                if (status != RUNNING)
                    throw Exception(
                        "exchange receiver map can not be initialized, because the task is not in running state");
            }
            return true;
        });
    }
    catch (...)
    {
        std::lock_guard lock(mtx);
        if (status != RUNNING)
            throw Exception("exchange receiver map can not be initialized, because the task is not in running state");
        receiver_set = std::move(receiver_set_local);
        throw;
    }
    {
        std::lock_guard lock(mtx);
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

    MPPTaskId receiver_id(request->receiver_meta());
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

MemoryTracker * MPPTask::getMemoryTracker() const
{
    if (process_list_entry_holder.process_list_entry != nullptr)
        return process_list_entry_holder.process_list_entry->get().getMemoryTrackerPtr().get();
    return nullptr;
}

void MPPTask::unregisterTask()
{
    if (is_registered)
    {
        auto [result, reason] = manager->unregisterTask(id, getErrString());
        if (result)
            LOG_DEBUG(log, "task unregistered");
        else
            LOG_WARNING(log, "task failed to unregister, reason: {}", reason);
    }
}

void MPPTask::initQueryOperatorSpillContexts(
    const std::shared_ptr<QueryOperatorSpillContexts> & mpp_query_operator_spill_contexts)
{
    assert(mpp_query_operator_spill_contexts != nullptr);
    dag_context->setQueryOperatorSpillContexts(mpp_query_operator_spill_contexts);
}

void MPPTask::initProcessListEntry(const std::shared_ptr<ProcessListEntry> & query_process_list_entry)
{
    /// all the mpp tasks of the same mpp query shares the same process list entry
    assert(query_process_list_entry != nullptr);
    process_list_entry_holder.process_list_entry = query_process_list_entry;
    dag_context->setProcessListEntry(query_process_list_entry);
    context->setProcessListElement(&query_process_list_entry->get());
    current_memory_tracker = getMemoryTracker();
}

void MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    dag_req = getDAGRequestFromStringWithRetry(task_request.encoded_plan());
    TMTContext & tmt_context = context->getTMTContext();
    /// MPP task will only use key ranges in mpp::DispatchTaskRequest::regions/mpp::DispatchTaskRequest::table_regions.
    /// The ones defined in tipb::TableScan will never be used and can be removed later.
    TablesRegionsInfo tables_regions_info
        = TablesRegionsInfo::create(task_request.regions(), task_request.table_regions(), tmt_context);
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
            throw TiFlashException(
                "Failed to decode task meta info in ExchangeSender",
                Errors::Coprocessor::BadRequest);
        }
        is_root_mpp_task = task_meta.task_id() == -1;
    }
    dag_context = std::make_unique<DAGContext>(dag_req, task_request.meta(), is_root_mpp_task);
    dag_context->log = log;
    dag_context->tables_regions_info = std::move(tables_regions_info);
    dag_context->tidb_host = context->getClientInfo().current_address.toString();

    context->setDAGContext(dag_context.get());

    injectFailPointBeforeRegisterMPPTask(dag_context->isRootMPPTask());
    auto [result, reason] = manager->registerTask(this);
    if (!result)
    {
        throw TiFlashException(
            fmt::format("Failed to register MPP Task {}, reason: {}", id.toString(), reason),
            Errors::Coprocessor::Internal);
    }

    injectFailPointBeforeRegisterTunnel(dag_context->isRootMPPTask());
    registerTunnels(task_request);

    LOG_DEBUG(log, "begin to make the task {} public", id.toString());

    injectFailPointBeforeMakeMPPTaskPublic(dag_context->isRootMPPTask());
    std::tie(result, reason) = manager->makeTaskActive(shared_from_this());
    if (!result)
    {
        throw TiFlashException(
            fmt::format("Failed to make MPP Task {} public: {}", id.toString(), reason),
            Errors::Coprocessor::BadRequest);
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
    RUNTIME_ASSERT(
        current_memory_tracker == getMemoryTracker(),
        log,
        "The current memory tracker is not set correctly for MPPTask::runImpl");
    if (!switchStatus(INITIALIZING, RUNNING))
    {
        LOG_WARNING(log, "task not in initializing state, skip running");
        unregisterTask();
        return;
    }

    Stopwatch stopwatch;
    const auto & resource_group = dag_context->getResourceGroupName();
    GET_METRIC(tiflash_coprocessor_request_count, type_run_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Increment();
    GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_handling_mpp_task_run, resource_group).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_run_mpp_task).Observe(stopwatch.elapsedSeconds());
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_handling_mpp_task_run, resource_group).Decrement();
    });

    // set cancellation hook
    context->setCancellationHook([this] { return is_cancelled.load(); });

    String err_msg;
    try
    {
        LOG_DEBUG(log, "task starts preprocessing");
        preprocess();
        auto time_cost_in_preprocess_ms = stopwatch.elapsedMilliseconds();
        LOG_DEBUG(log, "task preprocess done");
        schedule_entry.setNeededThreads(estimateCountOfNewThreads());
        LOG_DEBUG(
            log,
            "Estimate new thread count of query: {} including tunnel_threads: {}, receiver_threads: {}",
            schedule_entry.getNeededThreads(),
            dag_context->tunnel_set->getExternalThreadCnt(),
            new_thread_count_of_mpp_receiver);

        scheduleOrWait();

        auto time_cost_in_schedule_ms = stopwatch.elapsedMilliseconds() - time_cost_in_preprocess_ms;
        LOG_INFO(
            log,
            "task starts running, time cost in schedule: {} ms, time cost in preprocess: {} ms",
            time_cost_in_schedule_ms,
            time_cost_in_preprocess_ms);
        if (status.load() != RUNNING)
        {
            /// when task is in running state, canceling the task will call sendCancelToQuery to do the cancellation, however
            /// if the task is cancelled during preprocess, sendCancelToQuery may just be ignored because the processlist of
            /// current task is not registered yet, so need to check the task status explicitly
            throw Exception("task not in running state, may be cancelled");
        }
        mpp_task_statistics.start();
        dag_context->registerTaskOperatorSpillContexts();

#ifndef NDEBUG
        if (isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_root_task_run);
        }
        else
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_non_root_task_run);
        }
#endif

        auto result = query_executor_holder->execute(time_cost_in_schedule_ms);
        auto log_level = Poco::Message::PRIO_DEBUG;
        if (!result.is_success || status != RUNNING)
            log_level = Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(
            log,
            log_level,
            "mpp task finish execute, is_success: {}, status: {}",
            result.is_success,
            magic_enum::enum_name(status.load()));
        auto cpu_time_ns = query_executor_holder->collectCPUTimeNs();
        auto cpu_ru = cpuTimeToRU(cpu_time_ns);
        auto read_bytes = dag_context->getReadBytes();
        auto read_ru = bytesToRU(read_bytes);
        LOG_DEBUG(log, "mpp finish with request unit: cpu={} read={}", cpu_ru, read_ru);
        GET_RESOURCE_GROUP_METRIC(tiflash_compute_request_unit, type_mpp, dag_context->getResourceGroupName())
            .Increment(cpu_ru + read_ru);
        mpp_task_statistics.setRUInfo(
            RUConsumption{.cpu_ru = cpu_ru, .cpu_time_ns = cpu_time_ns, .read_ru = read_ru, .read_bytes = read_bytes});
        mpp_task_statistics.setExtraInfo(query_executor_holder->getExtraJsonInfo());

        mpp_task_statistics.collectRuntimeStatistics();

        auto runtime_statistics = query_executor_holder->getRuntimeStatistics();
        LOG_DEBUG(
            log,
            "finish with {} seconds, {} rows, {} blocks, {} bytes",
            runtime_statistics.execution_time_ns / static_cast<double>(1000000000),
            runtime_statistics.rows,
            runtime_statistics.blocks,
            runtime_statistics.bytes);
        if (likely(result.is_success))
        {
            /// Need to finish writing before closing the receiver.
            /// For example, for the query with limit, calling `finishWrite` first to ensure that the limit executor on the TiDB side can end normally,
            /// otherwise the upstream MPPTasks will fail because of the closed receiver and then passing the error to TiDB.
            ///
            ///               ┌──tiflash(limit)◄─┬─tiflash(no limit)
            /// tidb(limit)◄──┼──tiflash(limit)◄─┼─tiflash(no limit)
            ///               └──tiflash(limit)◄─┴─tiflash(no limit)

            // finish MPPTunnel
            finishWrite();
            // finish receiver
            receiver_set->close();
        }
        result.verify();
    }
    catch (...)
    {
        auto catch_err_msg = getCurrentExceptionMessage(true, true);
        err_msg = err_msg.empty() ? catch_err_msg : fmt::format("{}, {}", err_msg, catch_err_msg);
    }

    if (err_msg.empty())
    {
        reportStatus("");
        if (switchStatus(RUNNING, FINISHED))
            LOG_DEBUG(log, "finish task");
        else
            LOG_WARNING(log, "finish task which is in {} state", magic_enum::enum_name(status.load()));
        if (status == FINISHED)
        {
            // todo when error happens, should try to update the metrics if it is available
            if (auto throughput = dag_context->getTableScanThroughput(); throughput.first)
                GET_METRIC(tiflash_storage_logical_throughput_bytes).Observe(throughput.second);
            /// note that memory_tracker is shared by all the mpp tasks, the peak memory usage is not accurate
            /// todo log executor level peak memory usage instead
            auto peak_memory = getMemoryTracker()->getPeak();
            mpp_task_statistics.setMemoryPeak(peak_memory);
        }
    }
    else
    {
        /// trim the stack trace to avoid too many useless information
        String trimmed_err_msg = err_msg;
        trimStackTrace(trimmed_err_msg);
        /// tidb replaces root tasks' error message with the first error message it received, if root task completed successfully, error messages will be ignored.
        reportStatus(trimmed_err_msg);
        if (status == RUNNING)
        {
            LOG_ERROR(log, "task running meets error: {}", err_msg);
            try
            {
                handleError(trimmed_err_msg);
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

// TODO: include warning messages in report also when MPPDataPacket support warnings
void MPPTask::reportStatus(const String & err_msg)
{
    if (!ReportStatusToCoordinator(meta.mpp_version(), meta.coordinator_address()))
        return;

    bool report_execution_summary = dag_context->collect_execution_summaries
        && ReportExecutionSummaryToCoordinator(meta.mpp_version(), meta.report_execution_summary());
    // Only report status when err happened or need to report execution summary
    if (err_msg.empty() && !report_execution_summary)
        return;

    try
    {
        mpp::ReportTaskStatusRequest req;
        mpp::TaskMeta * req_meta = req.mutable_meta();
        req_meta->CopyFrom(meta);

        if (report_execution_summary)
        {
            tipb::TiFlashExecutionInfo execution_info = mpp_task_statistics.genTiFlashExecutionInfo();
            if unlikely (!execution_info.SerializeToString(req.mutable_data()))
            {
                LOG_ERROR(log, "Failed to serialize TiFlash execution info");
                return;
            }
        }

        if (!err_msg.empty())
        {
            mpp::Error * err = req.mutable_error();
            err->set_code(ErrorCodes::UNKNOWN_EXCEPTION);
            err->set_msg(err_msg);
        }

        auto * cluster = context->getTMTContext().getKVCluster();
        pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(ReportMPPTaskStatus)> rpc(
            cluster->rpc_client,
            meta.coordinator_address());
        grpc::ClientContext client_context;
        rpc.setClientContext(client_context, /*timeout=*/3);
        mpp::ReportTaskStatusResponse resp;
        auto rpc_status = rpc.call(&client_context, req, &resp);
        if (!rpc_status.ok())
        {
            throw Exception(rpc.errMsg(rpc_status));
        }
        if (resp.has_error())
        {
            LOG_WARNING(log, "ReportMPPTaskStatus resp error: {}", resp.error().msg());
        }
    }
    catch (...)
    {
        std::string local_err_msg = getCurrentExceptionMessage(true);
        LOG_ERROR(log, "Failed to ReportMPPTaskStatus to {}, due to {}", meta.coordinator_address(), local_err_msg);
    }
}

void MPPTask::handleError(const String & error_msg)
{
    /// Not call abortMPPGather to avoid issue https://github.com/pingcap/tiflash/issues/7177
    // auto updated_msg = fmt::format("From {}: {}", id.toString(), error_msg);
    //manager->abortMPPGather(id.gather_id, updated_msg, AbortType::ONERROR);
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
            /// abort mpptunnels first because if others components are aborted
            /// first, the mpptunnels may see an error caused by the abort, which is not
            /// the original error
            setErrString(message);
            abortTunnels(message, false);
            abortReceivers();
            abortQueryExecutor();
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
    return (*query_executor)->estimateNewThreadCount() + 1 + dag_context->tunnel_set->getExternalThreadCnt()
        + new_thread_count_of_mpp_receiver;
}

} // namespace DB
