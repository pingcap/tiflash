#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
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
} // namespace FailPoints

String MPPTaskId::toString() const
{
    return fmt::format("[{},{}]", start_ts, task_id);
}

MPPTask::MPPTask(const mpp::TaskMeta & meta_, const Context & context_)
    : context(context_)
    , meta(meta_)
    , log(&Logger::get(fmt::format("task {}", meta_.task_id())))
{
    id.start_ts = meta.start_ts();
    id.task_id = meta.task_id();
}

MPPTask::~MPPTask()
{
    /// MPPTask maybe destructed by different thread, set the query memory_tracker
    /// to current_memory_tracker in the destructor
    current_memory_tracker = memory_tracker;
    closeAllTunnel("");
    LOG_DEBUG(log, "finish MPPTask: " << id.toString());
}

void MPPTask::closeAllTunnel(const String & reason)
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

std::vector<RegionInfo> MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    auto start_time = Clock::now();
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
    RegionInfoMap regions;
    RegionInfoList retry_regions;
    for (auto & r : task_request.regions())
    {
        auto res = regions.emplace(r.region_id(),
                                   RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(), CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
        if (!res.second)
        {
            retry_regions.emplace_back(RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(), CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
        }
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
    MPPTunnelSetPtr tunnel_set = std::make_shared<MPPTunnelSet>();
    const auto & exchangeSender = dag_req->root_executor().exchange_sender();
    std::chrono::seconds timeout(task_request.timeout());
    for (int i = 0; i < exchangeSender.encoded_task_meta_size(); i++)
    {
        // exchange sender will register the tunnels and wait receiver to found a connection.
        mpp::TaskMeta task_meta;
        task_meta.ParseFromString(exchangeSender.encoded_task_meta(i));
        MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>(task_meta, task_request.meta(), timeout, this->shared_from_this());
        LOG_DEBUG(log, "begin to register the tunnel " << tunnel->id());
        registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
        tunnel_set->addTunnel(tunnel);
        if (!dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
        }
    }

    // register task.
    TMTContext & tmt_context = context.getTMTContext();
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

    DAGQuerySource dag(context, regions, retry_regions, *dag_req, true);

    // read index , this may take a long time.
    io = executeQuery(dag, context, false, QueryProcessingStage::Complete);

    // get partition column ids
    auto part_keys = exchangeSender.partition_keys();
    std::vector<Int64> partition_col_id;
    TiDB::TiDBCollators collators;
    /// in case TiDB is an old version, it has not collation info
    bool has_collator_info = exchangeSender.types_size() != 0;
    if (has_collator_info && part_keys.size() != exchangeSender.types_size())
    {
        throw TiFlashException(std::string(__PRETTY_FUNCTION__)
                                   + ": Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB new collation is "
                                     "enabled",
                               Errors::Coprocessor::BadRequest);
    }
    for (int i = 0; i < part_keys.size(); i++)
    {
        const auto & expr = part_keys[i];
        assert(isColumnExpr(expr));
        auto column_index = decodeDAGInt64(expr.val());
        partition_col_id.emplace_back(column_index);
        if (has_collator_info && getDataTypeByFieldType(expr.field_type())->isString())
        {
            collators.emplace_back(getCollatorFromFieldType(exchangeSender.types(i)));
        }
        else
        {
            collators.emplace_back(nullptr);
        }
    }
    // construct writer
    std::unique_ptr<DAGResponseWriter> response_writer
        = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(tunnel_set, partition_col_id, collators, exchangeSender.tp(), context.getSettings().dag_records_per_chunk, dag.getEncodeType(), dag.getResultFieldTypes(), *dag_context);
    BlockOutputStreamPtr squash_stream = std::make_shared<DAGBlockOutputStream>(io.in->getHeader(), std::move(response_writer));
    io.out = std::make_shared<SquashingBlockOutputStream>(squash_stream, 20000, 0);
    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context->compile_time_ns = compile_time_ns;

    return dag_context->retry_regions;
}

void MPPTask::runImpl()
{
    auto old_status = static_cast<Int32>(INITIALIZING);
    if (!status.compare_exchange_strong(old_status, static_cast<Int32>(RUNNING)))
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
    LOG_INFO(log, "task starts running");
    auto from = io.in;
    auto to = io.out;
    try
    {
        from->readPrefix();
        to->writePrefix();
        LOG_DEBUG(log, "begin read ");

        size_t count = 0;

        while (Block block = from->read())
        {
            count += block.rows();
            to->write(block);
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

        /// For outputting additional information in some formats.
        if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(from.get()))
        {
            if (input->getProfileInfo().hasAppliedLimit())
                to->setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

            to->setTotals(input->getTotals());
            to->setExtremes(input->getExtremes());
        }

        from->readSuffix();
        to->writeSuffix();

        finishWrite();

        LOG_DEBUG(log, "finish write with " + std::to_string(count) + " rows");
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "task running meets error " << e.displayText() << " Stack Trace : " << e.getStackTrace().toString());
        writeErrToAllTunnel(e.displayText());
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "task running meets error " << e.what());
        writeErrToAllTunnel(e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "unrecovered error");
        writeErrToAllTunnel("unrecovered fatal error");
    }
    auto throughput = dag_context->getTableScanThroughput();
    if (throughput.first)
        GET_METRIC(tiflash_storage_logical_throughput_bytes).Observe(throughput.second);
    LOG_INFO(log, "task ends, time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
    auto process_info = context.getProcessListElement()->getInfo();
    auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
    GET_METRIC(tiflash_coprocessor_request_memory_usage, type_run_mpp_task).Observe(peak_memory);
    unregisterTask();
    status = FINISHED;
}

void MPPTask::writeErrToAllTunnel(const String & e)
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
            tryLogCurrentException(log, "Failed to write error " + e + " to tunnel: " + it.second->id());
        }
    }
}

void MPPTask::cancel(const String & reason)
{
    auto current_status = status.exchange(CANCELLED);
    if (current_status == FINISHED || current_status == CANCELLED)
    {
        if (current_status == FINISHED)
            status = FINISHED;
        return;
    }
    LOG_WARNING(log, "Begin cancel task: " + id.toString());
    /// step 1. cancel query streams if it is running
    if (current_status == RUNNING)
        context.getProcessList().sendCancelToQuery(context.getCurrentQueryId(), context.getClientInfo().current_user, true);
    /// step 2. write Error msg and close the tunnel.
    /// Here we use `closeAllTunnel` because currently, `cancel` is a query level cancel, which
    /// means if this mpp task is cancelled, all the mpp tasks belonging to the same query are
    /// cancelled at the same time, so there is no guarantee that the tunnel can be connected.
    closeAllTunnel(reason);
    LOG_WARNING(log, "Finish cancel task: " + id.toString());
}

} // namespace DB
