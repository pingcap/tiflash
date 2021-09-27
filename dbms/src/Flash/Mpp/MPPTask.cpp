#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
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
extern const char hang_in_execution[];
extern const char exception_before_mpp_register_non_root_mpp_task[];
extern const char exception_before_mpp_register_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_root_mpp_task[];
extern const char exception_during_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
extern const char exception_during_mpp_write_err_to_tunnel[];
extern const char force_no_local_region_for_mpp_task[];
} // namespace FailPoints

String MPPTaskId::toString() const
{
    return fmt::format("[{},{}]", start_ts, task_id);
}

MPPTask::MPPTask(const mpp::TaskMeta & meta_, const Context & context_)
    : context(context_)
    , meta(meta_)
    , log(std::make_shared<LogWithPrefix>(
          &Poco::Logger::get("MPPTask"),
          fmt::format("[task {} query {}] ", meta.task_id(), meta.start_ts())))
{
    id.start_ts = meta.start_ts();
    id.task_id = meta.task_id();
}

MPPTask::~MPPTask()
{
    /// MPPTask maybe destructed by different thread, set the query memory_tracker
    /// to current_memory_tracker in the destructor
    current_memory_tracker = memory_tracker;
    closeAllTunnels("");
    LOG_DEBUG(log, "finish MPPTask: " << id.toString());
}

void MPPTask::closeAllTunnels(const String & reason)
{
    for (auto & it : tunnel_map)
    {
        it.second->close(reason);
    }
}

void MPPTask::finishWrite()
{
    for (auto it : tunnel_map)
    {
        it.second->writeDone();
    }
}

void MPPTask::run()
{
    memory_tracker = current_memory_tracker;
    std::thread worker(&MPPTask::runImpl, this->shared_from_this());
    worker.detach();
}

void MPPTask::registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel)
{
    if (status == CANCELLED)
        throw Exception("the tunnel " + tunnel->id() + " can not been registered, because the task is cancelled");

    if (tunnel_map.find(id) != tunnel_map.end())
        throw Exception("the tunnel " + tunnel->id() + " has been registered");

    tunnel_map[id] = tunnel;
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

    MPPTaskId id{request->receiver_meta().start_ts(), request->receiver_meta().task_id()};
    std::map<MPPTaskId, MPPTunnelPtr>::iterator it = tunnel_map.find(id);
    if (it == tunnel_map.end())
    {
        auto err_msg = fmt::format(
            "can't find tunnel ({} + {})",
            request->sender_meta().task_id(),
            request->receiver_meta().task_id());
        return {nullptr, err_msg};
    }
    return {it->second, ""};
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

bool needRemoteRead(const RegionInfo & region_info, const TMTContext & tmt_context)
{
    fiu_do_on(FailPoints::force_no_local_region_for_mpp_task, { return true; });
    RegionPtr current_region = tmt_context.getKVStore()->getRegion(region_info.region_id);
    if (current_region == nullptr || current_region->peerState() != raft_serverpb::PeerState::Normal)
        return true;
    auto meta_snap = current_region->dumpRegionMetaSnapshot();
    if (meta_snap.ver != region_info.region_version)
        return true;
    return false;
}

std::vector<RegionInfo> MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    dag_req = std::make_unique<tipb::DAGRequest>();
    if (!dag_req->ParseFromString(task_request.encoded_plan()))
    {
        /// ParseFromString will use the default recursion limit, which is 100 to decode the plan, if the plan tree is too deep,
        /// it may exceed this limit, so just try again by double the recursion limit
        ::google::protobuf::io::CodedInputStream coded_input_stream(
            reinterpret_cast<const UInt8 *>(task_request.encoded_plan().data()),
            task_request.encoded_plan().size());
        coded_input_stream.SetRecursionLimit(::google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() * 2);
        if (!dag_req->ParseFromCodedStream(&coded_input_stream))
        {
            /// just return error if decode failed this time, because it's really a corner case, and even if we can decode the plan
            /// successfully by using a very large value of the recursion limit, it is kinds of meaningless because the runtime
            /// performance of this task may be very bad if the plan tree is too deep
            throw TiFlashException(
                std::string(__PRETTY_FUNCTION__) + ": Invalid encoded plan, the most likely is that the plan tree is too deep",
                Errors::Coprocessor::BadRequest);
        }
    }
    TMTContext & tmt_context = context.getTMTContext();
    for (auto & r : task_request.regions())
    {
        RegionInfo region_info(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(), CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr);
        if (region_info.key_ranges.empty())
        {
            throw TiFlashException(
                "Income key ranges is empty for region: " + std::to_string(region_info.region_id),
                Errors::Coprocessor::BadRequest);
        }
        /// TiFlash does not support regions with duplicated region id, so for regions with duplicated
        /// region id, only the first region will be treated as local region
        bool duplicated_region = local_regions.find(region_info.region_id) != local_regions.end();

        if (duplicated_region || needRemoteRead(region_info, tmt_context))
            remote_regions.push_back(region_info);
        else
            local_regions.insert(std::make_pair(region_info.region_id, region_info));
    }
    // set schema ver and start ts.
    auto schema_ver = task_request.schema_ver();
    auto start_ts = task_request.meta().start_ts();

    context.setSetting("read_tso", start_ts);
    context.setSetting("schema_version", schema_ver);
    if (unlikely(task_request.timeout() < 0))
    {
        /// this is only for test
        context.setSetting("mpp_task_timeout", (Int64)5);
        context.setSetting("mpp_task_running_timeout", (Int64)10);
    }
    else
    {
        context.setSetting("mpp_task_timeout", task_request.timeout());
        if (task_request.timeout() > 0)
        {
            /// in the implementation, mpp_task_timeout is actually the task writing tunnel timeout
            /// so make the mpp_task_running_timeout a little bigger than mpp_task_timeout
            context.setSetting("mpp_task_running_timeout", task_request.timeout() + 30);
        }
    }
    context.getTimezoneInfo().resetByDAGRequest(*dag_req);

    dag_context = std::make_unique<DAGContext>(*dag_req, task_request.meta());
    dag_context->mpp_task_log = log;
    context.setDAGContext(dag_context.get());

    if (dag_context->isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_tunnel_for_non_root_mpp_task);
    }

    // register tunnels
    tunnel_set = std::make_shared<MPPTunnelSet>();
    const auto & exchangeSender = dag_req->root_executor().exchange_sender();
    std::chrono::seconds timeout(task_request.timeout());

    auto task_cancelled_callback = [task = std::weak_ptr<MPPTask>(shared_from_this())] {
        auto sp = task.lock();
        return sp && sp->getStatus() == CANCELLED;
    };

    for (int i = 0; i < exchangeSender.encoded_task_meta_size(); i++)
    {
        // exchange sender will register the tunnels and wait receiver to found a connection.
        mpp::TaskMeta task_meta;
        task_meta.ParseFromString(exchangeSender.encoded_task_meta(i));
        MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>(task_meta, task_request.meta(), timeout, task_cancelled_callback, context.getSettings().max_threads);
        LOG_DEBUG(log, "begin to register the tunnel " << tunnel->id());
        registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
        tunnel_set->addTunnel(tunnel);
        if (!dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
        }
    }
    dag_context->tunnel_set = tunnel_set;
    // register task.
    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_DEBUG(log, "begin to register the task " << id.toString());

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

    return remote_regions;
}

void MPPTask::preprocess()
{
    auto start_time = Clock::now();
    DAGQuerySource dag(context, local_regions, remote_regions, *dag_req, log, true);
    io = executeQuery(dag, context, false, QueryProcessingStage::Complete);
    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context->compile_time_ns = compile_time_ns;
}

void MPPTask::runImpl()
{
    if (!switchStatus(INITIALIZING, RUNNING))
    {
        LOG_WARNING(log, "task not in initializing state, skip running");
        return;
    }

    current_memory_tracker = memory_tracker;
    Stopwatch stopwatch;
    GET_METRIC(tiflash_coprocessor_request_count, type_run_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_run_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_run_mpp_task).Observe(stopwatch.elapsedSeconds());
    });
    String err_msg;
    LOG_INFO(log, "task starts running");
    try
    {
        preprocess();
        if (status.load() != RUNNING)
        {
            /// when task is in running state, cancel the task will call sendCancelToQuery to do the cancellation, however
            /// if the task is cancelled during preprocess, sendCancelToQuery may just be ignored because the processlist of
            /// current task is not registered yet, so need to check the task status explicitly
            throw Exception("task not in running state, maybe is cancelled");
        }
        auto from = io.in;
        from->readPrefix();
        LOG_DEBUG(log, "begin read ");

        size_t count = 0;

        while (Block block = from->read())
        {
            count += block.rows();
            FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
            if (dag_context->isRootMPPTask())
            {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_root_task_run);
            }
            else
            {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_non_root_task_run);
            }
        }

        from->readSuffix();
        finishWrite();
        LOG_DEBUG(log, "finish write with " + std::to_string(count) + " rows");
    }
    catch (Exception & e)
    {
        err_msg = e.displayText();
        LOG_ERROR(log, "task running meets error: " << err_msg << " Stack Trace : " << e.getStackTrace().toString());
    }
    catch (std::exception & e)
    {
        err_msg = e.what();
        LOG_ERROR(log, "task running meets error: " << err_msg);
    }
    catch (...)
    {
        err_msg = "unrecovered error";
        LOG_ERROR(log, "task running meets error: " << err_msg);
    }
    if (err_msg.empty())
    {
        // todo when error happens, should try to update the metrics if it is available
        auto throughput = dag_context->getTableScanThroughput();
        if (throughput.first)
            GET_METRIC(tiflash_storage_logical_throughput_bytes).Observe(throughput.second);
        auto process_info = context.getProcessListElement()->getInfo();
        auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
        GET_METRIC(tiflash_coprocessor_request_memory_usage, type_run_mpp_task).Observe(peak_memory);
    }
    else
    {
        writeErrToAllTunnels(err_msg);
    }
    LOG_INFO(log, "task ends, time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
    unregisterTask();

    if (switchStatus(RUNNING, FINISHED))
        LOG_INFO(log, "finish task");
    else
        LOG_WARNING(log, "finish task which was cancelled before");
}

void MPPTask::writeErrToAllTunnels(const String & e)
{
    for (auto & it : tunnel_map)
    {
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_write_err_to_tunnel);
            it.second->write(getPacketWithError(e), true);
        }
        catch (...)
        {
            it.second->close("Failed to write error msg to tunnel");
            tryLogCurrentException(log->getLog(), "Failed to write error " + e + " to tunnel: " + it.second->id());
        }
    }
}

void MPPTask::cancel(const String & reason)
{
    LOG_WARNING(log, "Begin cancel task: " + id.toString());
    while (true)
    {
        auto previous_status = status.load();
        if (previous_status == FINISHED || previous_status == CANCELLED)
        {
            LOG_WARNING(log, "task already " << (previous_status == FINISHED ? "finished" : "cancelled"));
            return;
        }
        else if (previous_status == INITIALIZING && switchStatus(INITIALIZING, CANCELLED))
        {
            closeAllTunnels(reason);
            unregisterTask();
            LOG_WARNING(log, "Finish cancel task from uninitialized");
            return;
        }
        else if (previous_status == RUNNING && switchStatus(RUNNING, CANCELLED))
        {
            context.getProcessList().sendCancelToQuery(context.getCurrentQueryId(), context.getClientInfo().current_user, true);
            closeAllTunnels(reason);
            /// runImpl is running, leave remaining work to runImpl
            LOG_WARNING(log, "Finish cancel task from running");
            return;
        }
    }
}

bool MPPTask::switchStatus(TaskStatus from, TaskStatus to)
{
    return status.compare_exchange_strong(from, to);
}

} // namespace DB
