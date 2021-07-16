#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

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
extern const char exception_before_mpp_non_root_task_run[];
extern const char exception_before_mpp_root_task_run[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
extern const char exception_during_mpp_write_err_to_tunnel[];
extern const char exception_during_mpp_close_tunnel[];
} // namespace FailPoints

bool MPPTaskProgress::isTaskHanging(const Context & context)
{
    bool ret = false;
    auto current_progress_value = current_progress.load();
    if (current_progress_value != progress_on_last_check)
    {
        /// make some progress
        found_no_progress = false;
    }
    else
    {
        /// no progress
        if (!found_no_progress)
        {
            /// first time on no progress
            found_no_progress = true;
            epoch_when_found_no_progress = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
        }
        else
        {
            /// no progress for a while, check timeout
            auto no_progress_duration
                = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count() - epoch_when_found_no_progress;
            auto timeout_threshold = current_progress_value == 0 ? context.getSettingsRef().mpp_task_waiting_timeout
                                                                 : context.getSettingsRef().mpp_task_running_timeout;
            if (no_progress_duration > timeout_threshold)
                ret = true;
        }
    }
    progress_on_last_check = current_progress_value;
    return ret;
}

//void MPPTunnel::close(const String & reason)
//{
//    std::unique_lock<std::mutex> lk(mu);
//    if (finished)
//        return;
//    if (connected)
//    {
//        try
//        {
//            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_close_tunnel);
//            mpp::MPPDataPacket data;
//            auto err = new mpp::Error();
//            err->set_msg(reason);
//            data.set_allocated_error(err);
//            writer->Write(data);
//        }
//        catch (...)
//        {
//            tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
//        }
//    }
//    finished = true;
//    cv_for_finished.notify_all();
//}

void MPPTask::unregisterTask()
{
    if (manager != nullptr)
    {
        LOG_DEBUG(log, "task unregistered");
        manager->unregisterTask(task_id);
    }
    else
    {
        LOG_ERROR(log, "task manager is unset");
    }
}

std::unordered_map<RegionVerID, RegionInfo> MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    //    auto start_time = Clock::now();
    dag_req = std::make_unique<tipb::DAGRequest>();
    if (!dag_req->ParseFromString(task_request.encoded_plan()))
    {
        /// ParseFromString will use the default recursion limit, which is 100 to decode the plan, if the plan tree is too deep,
        /// it may exceed this limit, so just try again by double the recursion limit
        ::google::protobuf::io::CodedInputStream coded_input_stream(
            reinterpret_cast<const UInt8 *>(task_request.encoded_plan().data()), task_request.encoded_plan().size());
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
    std::unordered_map<RegionVerID, RegionInfo> regions;
    for (auto & r : task_request.regions())
    {
        RegionVerID region_ver_id(r.region_id(), r.region_epoch().conf_ver(), r.region_epoch().version());
        auto res = regions.emplace(region_ver_id, RegionInfo(region_ver_id, CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
        if (!res.second)
            throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": contain duplicate region " + region_ver_id.toString(),
                Errors::Coprocessor::BadRequest);
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

    // register task.
    //    TMTContext & tmt_context = context.getTMTContext();
    //    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_DEBUG(log, "begin to register the task " << task_id.toString());

    if (dag_context->isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_root_mpp_task);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_register_non_root_mpp_task);
    }

    return regions;
}

//    if (!task_manager->registerTask(shared_from_this()))
//    {
//        throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": Failed to register MPP Task", Errors::Coprocessor::BadRequest);
//    }

std::vector<RegionInfo> MPPTask::initStreams(
    const mpp::DispatchTaskRequest & task_request, std::unordered_map<RegionVerID, RegionInfo> & regions)
{
    auto start_time = Clock::now();

    DAGQuerySource dag(context, regions, *dag_req, true);

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
        MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>(task_meta, task_request.meta(), timeout);
        LOG_DEBUG(log, "begin to register the tunnel " << tunnel->tunnel_id);
        registerTunnel(MPPTaskId(task_meta), tunnel);
        tunnel_set->tunnels.emplace_back(tunnel);
        if (!dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task);
        }
    }
    // read index , this may take a long time.
    io = executeQuery(dag, context, false, QueryProcessingStage::Complete);

    // get partition column ids
    auto part_keys = exchangeSender.partition_keys();
    std::vector<Int64> partition_col_id;
    for (const auto & expr : part_keys)
    {
        assert(isColumnExpr(expr));
        auto column_index = decodeDAGInt64(expr.val());
        partition_col_id.emplace_back(column_index);
    }
    // construct writer
    std::unique_ptr<DAGResponseWriter> response_writer
        = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(tunnel_set, partition_col_id, exchangeSender.tp(),
            context.getSettings().dag_records_per_chunk, dag.getEncodeType(), dag.getResultFieldTypes(), *dag_context);
    BlockOutputStreamPtr squash_stream = std::make_shared<DAGBlockOutputStream>(io.in->getHeader(), std::move(response_writer));
    io.out = std::make_shared<SquashingBlockOutputStream>(squash_stream, 20000, 0);
    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context->compile_time_ns = compile_time_ns;

    return dag_context->retry_regions;
}

String taskStatusToString(TaskStatus ts)
{
    switch (ts)
    {
        case INITIALIZING:
            return "initializing";
        case RUNNING:
            return "running";
            //        case FINISHED:
            //            return "finished";
        case CANCELLED:
            return "cancelled";
        default:
            return "unknown";
    }
}
void MPPTask::runImpl(const MPPTaskProxyPtr & task_proxy)
{
    current_memory_tracker = memory_tracker;
    Stopwatch stopwatch;
    SCOPE_EXIT(

        LOG_INFO(log, "task ends, time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
        auto process_info = context.getProcessListElement()->getInfo();
        auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
        GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_memory_usage, type_dispatch_mpp_task).Observe(peak_memory);

        unregisterTask(););


    if (task_proxy->hasCancelled() || Terminated::server_terminated)
    {
        LOG_WARNING(log, "task has been cancelled, skip running");
        return;
    }

    LOG_INFO(log, "task starts running");

    auto from = io.in;
    auto to = io.out;
    try
    {

        AsynchronousBlockInputStream async_in(from);
        async_in.readPrefix();

        to->writePrefix();
        LOG_DEBUG(log, "begin read ");

        size_t count = 0;
        int times = 0;
        while (true)
        {
            Block block;
            while (true)
            {
                if (task_proxy->hasCancelled() || Terminated::server_terminated)
                {
                    LOG_WARNING(log, "task has been cancelled, skip running");
                    return;
                }
                if (async_in.poll((!times) ? 20 : 100))
                {
                    /// There is the following result block.
                    block = async_in.read();
                    break;
                }

                times++;
            }

            // Here we make sure that all tunnels are connected to their writers before continue.

            while (!allTunnelConnected())
            {
                auto [cancelled, has_new_writer, tunnel_map] = task_proxy->getNewTunnelWriters();
                if (cancelled || Terminated::server_terminated)
                {
                    LOG_WARNING(log, "task has been cancelled, skip running");
                    return;
                }
                if (has_new_writer)
                    connectTunnelWriters(tunnel_map);
            }

            if (!block)
                break;

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

        // while (Block block = from->read())
        // {
        //     count += block.rows();
        //     to->write(block);
        //     FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
        //     if (dag_context->isRootMPPTask())
        //     {
        //         FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_root_task_run);
        //     }
        //     else
        //     {
        //         FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_non_root_task_run);
        //     }
        // }

        /// For outputting additional information in some formats.
        if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(from.get()))
        {
            if (input->getProfileInfo().hasAppliedLimit())
                to->setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

            to->setTotals(input->getTotals());
            to->setExtremes(input->getExtremes());
        }

        async_in.readSuffix();
        to->writeSuffix();

        //        finishWrite();

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
}

void MPPTask::writeErrToAllTunnel(const String & e)
{
    for (auto & it : tunnel_map)
    {
        MPPTunnelPtr tunnel = it.second;
        if (!(tunnel->connected))
            continue;
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_write_err_to_tunnel);
            mpp::MPPDataPacket data;
            auto err = new mpp::Error();
            err->set_msg(e);
            data.set_allocated_error(err);
            tunnel->write(data);
        }
        catch (...)
        {
            //            tunnel->close("Failed to write error msg to tunnel");
            tryLogCurrentException(log, "Failed to write error " + e + " to tunnel: " + tunnel->tunnel_id);
        }
    }
}

void MPPTask::handleError(String error)
{
    try
    {
        writeErrToAllTunnel(error);
        unregisterTask();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fail to handle error and clean task");
    }
}
// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    MPPTaskPtr task;
    MPPTaskProxyPtr task_proxy;
    Stopwatch stopwatch;
    try
    {
        MPPTaskId task_id(task_request.meta());
        task = std::make_shared<MPPTask>(task_id, context);
        task_proxy = std::make_shared<MPPTaskProxy>(task_id);

        auto regions = task->prepare(task_request);

        TMTContext & tmt_context = context.getTMTContext();
        auto task_manager = tmt_context.getMPPTaskManager();
        task_manager->registerTask(task, task_proxy);

        auto retry_regions = task->initStreams(task_request, regions);

        for (auto region : retry_regions)
        {
            auto * retry_region = response->add_retry_regions();
            retry_region->set_id(region.region_ver_id.id);
            retry_region->mutable_region_epoch()->set_conf_ver(region.region_ver_id.conf_ver);
            retry_region->mutable_region_epoch()->set_version(region.region_ver_id.ver);
        }
        if (task->dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_root_task_run);
        }
        else
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_non_root_task_run);
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.displayText());
        auto * err = response->mutable_error();
        err->set_msg(e.displayText());
        task->handleError(e.displayText());
        return grpc::Status::OK;
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.what());
        auto * err = response->mutable_error();
        err->set_msg(e.what());
        task->handleError(e.what());
        return grpc::Status::OK;
    }
    catch (...)
    {
        LOG_ERROR(log, "dispatch task meet fatal error");
        auto * err = response->mutable_error();
        err->set_msg("fatal error");
        task->handleError("fatal error");
        return grpc::Status::OK;
    }

    task->memory_tracker = current_memory_tracker;

    MPPTaskWorker worker(std::move(task_proxy), std::move(task));
    MPPTaskWorker::start(std::move(worker));

    LOG_INFO(log, "processing dispatch is over; the time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms");

    return grpc::Status::OK;
}

MPPTaskManager::MPPTaskManager() : log(&Logger::get("TaskManager")) {}

MPPTaskProxyPtr MPPTaskManager::findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg)
{
    MPPTaskId id(meta);
    std::map<MPPTaskId, MPPTaskProxyWeakPtr>::iterator it;
    bool cancelled = false;
    std::unique_lock<std::mutex> lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto query_it = mpp_query_map.find(id.start_ts);
        // TODO: how about the query has been cancelled in advance?
        if (query_it == mpp_query_map.end())
        {
            return false;
        }
        else if (query_it->second.to_be_cancelled)
        {
            /// if the query is cancelled, return true to stop waiting timeout.
            LOG_WARNING(log, "Query " + std::to_string(id.start_ts) + " is cancelled, all its tasks are invalid.");
            cancelled = true;
            return true;
        }
        it = query_it->second.task_map.find(id);
        return it != query_it->second.task_map.end();
    });
    if (cancelled)
    {
        errMsg = "Task [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] has been cancelled.";
        return {};
    }
    else if (!ret)
    {
        errMsg = "Can't find task [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] within "
            + DB::toString(timeout.count()) + " s.";
        return {};
    }
    auto task_proxy = it->second.lock();
    if (!task_proxy)
        errMsg = "Task already released [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] within "
            + DB::toString(timeout.count()) + " s.";
    return task_proxy;
}

void MPPTaskManager::cancelMPPQuery(UInt64 query_id, const String & reason)
{
    MPPQueryTaskSet task_set;
    {
        /// cancel task may take a long time, so first
        /// set a flag, so we can cancel task one by
        /// one without holding the lock
        std::lock_guard<std::mutex> lock(mu);
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end() || it->second.to_be_cancelled)
            return;
        it->second.to_be_cancelled = true;
        task_set = it->second;
        cv.notify_all();
    }
    LOG_WARNING(log, "Begin cancel query: " + std::to_string(query_id));
    std::stringstream ss;
    ss << "Remaining task in query " + std::to_string(query_id) + " are: ";

    for (auto task_it = task_set.task_map.rbegin(); task_it != task_set.task_map.rend(); task_it++)
    {
        ss << task_it->first.toString() << " ";
        auto task_proxy_weak_ptr = task_it->second;
        if (auto task_proxy_ptr = task_proxy_weak_ptr.lock())
        {
            task_proxy_ptr->cancelTask(reason);
        }
    }
    LOG_WARNING(log, ss.str());

    MPPQueryTaskSet cancelled_task_set;
    {
        std::lock_guard<std::mutex> lock(mu);
        /// just to double check the query still exists
        auto it = mpp_query_map.find(query_id);
        if (it != mpp_query_map.end())
        {
            /// hold the cancelled task set, so the mpp task will not be deconstruct when holding the
            /// `mu` of MPPTaskManager, otherwise it might cause deadlock
            cancelled_task_set.swap(it->second);
            mpp_query_map.erase(it);
        }
    }
    LOG_WARNING(log, "Finish cancel query: " + std::to_string(query_id));
}

bool MPPTaskManager::registerTask(const MPPTaskPtr & task, const MPPTaskProxyPtr & task_proxy)
{
    auto & task_id = task->task_id;
    std::unique_lock<std::mutex> lock(mu);
    const auto & it = mpp_query_map.find(task_id.start_ts);
    if (it != mpp_query_map.end() && it->second.to_be_cancelled)
    {
        LOG_WARNING(log, "Do not register task: " + task_id.toString() + " because the query is to be cancelled.");
        cv.notify_all();
        return false;
    }
    if (it != mpp_query_map.end() && it->second.task_map.find(task_id) != it->second.task_map.end())
    {
        throw Exception("The task " + task_id.toString() + " has been registered");
    }
    mpp_query_map[task_id.start_ts].task_map.emplace(task_id, MPPTaskProxyWeakPtr(task_proxy));
    task->manager = this;
    cv.notify_all();
    return true;
}

void MPPTaskManager::unregisterTask(const MPPTaskId & task_id)
{
    std::unique_lock<std::mutex> lock(mu);
    auto it = mpp_query_map.find(task_id.start_ts);
    if (it != mpp_query_map.end())
    {
        if (it->second.to_be_cancelled)
            return;
        auto task_it = it->second.task_map.find(task_id);
        if (task_it != it->second.task_map.end())
        {
            it->second.task_map.erase(task_it);
            if (it->second.task_map.empty())
                /// remove query task map if the task is the last one
                mpp_query_map.erase(it);
            return;
        }
    }
    LOG_ERROR(log, "The task " + task_id.toString() + " cannot be found and fail to unregister");
}

MPPTaskManager::~MPPTaskManager() {}

} // namespace DB
