#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
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

void MPPTunnel::close(const String & reason)
{
    std::unique_lock<std::mutex> lk(mu);
    if (finished)
        return;
    if (connected && !reason.empty())
    {
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_close_tunnel);
            mpp::MPPDataPacket data;
            auto err = new mpp::Error();
            err->set_msg(reason);
            data.set_allocated_error(err);
            if (!writer->Write(data))
                throw Exception("Failed to write err");
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to close tunnel: " + tunnel_id);
        }
    }
    finished = true;
    cv_for_finished.notify_all();
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
    getDAGRequestFromStringWithRetry(*dag_req, task_request.encoded_plan());
    RegionInfoMap regions;
    RegionInfoList retry_regions;
    for (auto & r : task_request.regions())
    {
        auto res = regions.emplace(r.region_id(),
            RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(),
                CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
        if (!res.second)
        {
            retry_regions.emplace_back(RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(),
                CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
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

    bool is_root_mpp_task = false;
    const auto & exchange_sender = dag_req->root_executor().exchange_sender();
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
    dag_context = std::make_unique<DAGContext>(*dag_req, task_request.meta(), is_root_mpp_task);
    context.setDAGContext(dag_context.get());

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
        LOG_DEBUG(log, "begin to register the tunnel " << tunnel->tunnel_id);
        registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
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
        case FINISHED:
            return "finished";
        case CANCELLED:
            return "cancelled";
        default:
            return "unknown";
    }
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
    catch (pingcap::Exception & e)
    {
        LOG_ERROR(log, "task running meets error " << e.message());
        writeErrToAllTunnel(e.message());
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
    LOG_INFO(log, "task ends, time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
    auto process_info = context.getProcessListElement()->getInfo();
    auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
    GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_memory_usage, type_dispatch_mpp_task).Observe(peak_memory);
    unregisterTask();
    status = FINISHED;
}

bool MPPTunnel::isTaskCancelled()
{
    auto sp = current_task.lock();
    return sp != nullptr && sp->status == CANCELLED;
}

void MPPTunnel::waitUntilConnect(std::unique_lock<std::mutex> & lk)
{
    if (timeout.count() > 0)
    {
        if (!cv_for_connected.wait_for(lk, timeout, [&]() { return connected || isTaskCancelled(); }))
        {
            throw Exception(tunnel_id + " is timeout");
        }
    }
    else
    {
        cv_for_connected.wait(lk, [&]() { return connected || isTaskCancelled(); });
    }
    if (!connected)
        throw Exception("MPPTunnel can not be connected because MPPTask is cancelled");
}

void MPPTask::writeErrToAllTunnel(const String & e)
{
    for (auto & it : tunnel_map)
    {
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_write_err_to_tunnel);
            mpp::MPPDataPacket data;
            auto err = new mpp::Error();
            err->set_msg(e);
            data.set_allocated_error(err);
            it.second->write(data, true);
        }
        catch (...)
        {
            it.second->close("Failed to write error msg to tunnel");
            tryLogCurrentException(log, "Failed to write error " + e + " to tunnel: " + it.second->tunnel_id);
        }
    }
}

bool MPPTask::isTaskHanging()
{
    if (status.load() == RUNNING)
        return task_progress.isTaskHanging(context);
    return false;
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

void MPPHandler::handleError(MPPTaskPtr task, String error)
{
    try
    {
        if (task != nullptr)
        {
            task->closeAllTunnel(error);
            task->unregisterTask();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fail to handle error and clean task");
    }
}
// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    MPPTaskPtr task = nullptr;
    try
    {
        Stopwatch stopwatch;
        task = std::make_shared<MPPTask>(task_request.meta(), context);

        auto retry_regions = task->prepare(task_request);
        for (auto region : retry_regions)
        {
            auto * retry_region = response->add_retry_regions();
            retry_region->set_id(region.region_id);
            retry_region->mutable_region_epoch()->set_conf_ver(region.region_conf_version);
            retry_region->mutable_region_epoch()->set_version(region.region_version);
        }
        if (task->dag_context->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_root_task_run);
        }
        else
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_non_root_task_run);
        }
        task->memory_tracker = current_memory_tracker;
        task->run();
        LOG_INFO(log, "processing dispatch is over; the time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms");
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.displayText());
        auto * err = response->mutable_error();
        err->set_msg(e.displayText());
        handleError(task, e.displayText());
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.what());
        auto * err = response->mutable_error();
        err->set_msg(e.what());
        handleError(task, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "dispatch task meet fatal error");
        auto * err = response->mutable_error();
        err->set_msg("fatal error");
        handleError(task, "fatal error");
    }
    return grpc::Status::OK;
}

MPPTaskManager::MPPTaskManager() : log(&Logger::get("TaskManager")) {}

MPPTaskPtr MPPTaskManager::findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg)
{
    MPPTaskId id{meta.start_ts(), meta.task_id()};
    std::map<MPPTaskId, MPPTaskPtr>::iterator it;
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
        return nullptr;
    }
    else if (!ret)
    {
        errMsg = "Can't find task [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] within "
            + DB::toString(timeout.count()) + " s.";
        return nullptr;
    }
    return it->second;
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
    // TODO: cancel tasks in order rather than issuing so many threads to cancel tasks
    std::vector<std::thread> cancel_workers;
    for (auto task_it = task_set.task_map.rbegin(); task_it != task_set.task_map.rend(); task_it++)
    {
        ss << task_it->first.toString() << " ";
        std::thread t(&MPPTask::cancel, task_it->second, std::ref(reason));
        cancel_workers.push_back(std::move(t));
    }
    LOG_WARNING(log, ss.str());
    for (auto & worker : cancel_workers)
    {
        worker.join();
    }
    MPPQueryTaskSet canceled_task_set;
    {
        std::lock_guard<std::mutex> lock(mu);
        /// just to double check the query still exists
        auto it = mpp_query_map.find(query_id);
        if (it != mpp_query_map.end())
        {
            /// hold the canceled task set, so the mpp task will not be deconstruct when holding the
            /// `mu` of MPPTaskManager, otherwise it might cause deadlock
            canceled_task_set = it->second;
            mpp_query_map.erase(it);
        }
    }
    LOG_WARNING(log, "Finish cancel query: " + std::to_string(query_id));
}

bool MPPTaskManager::registerTask(MPPTaskPtr task)
{
    std::unique_lock<std::mutex> lock(mu);
    const auto & it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end() && it->second.to_be_cancelled)
    {
        LOG_WARNING(log, "Do not register task: " + task->id.toString() + " because the query is to be cancelled.");
        cv.notify_all();
        return false;
    }
    if (it != mpp_query_map.end() && it->second.task_map.find(task->id) != it->second.task_map.end())
    {
        throw Exception("The task " + task->id.toString() + " has been registered");
    }
    mpp_query_map[task->id.start_ts].task_map.emplace(task->id, task);
    task->manager = this;
    cv.notify_all();
    return true;
}

void MPPTaskManager::unregisterTask(MPPTask * task)
{
    std::unique_lock<std::mutex> lock(mu);
    auto it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end())
    {
        if (it->second.to_be_cancelled)
            return;
        auto task_it = it->second.task_map.find(task->id);
        if (task_it != it->second.task_map.end())
        {
            it->second.task_map.erase(task_it);
            if (it->second.task_map.empty())
                /// remove query task map if the task is the last one
                mpp_query_map.erase(it);
            return;
        }
    }
    LOG_ERROR(log, "The task " + task->id.toString() + " cannot be found and fail to unregister");
}

MPPTaskManager::~MPPTaskManager() {}

} // namespace DB
