#include <Common/TiFlashMetrics.h>
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

bool MPPTaskProgress::isTaskHanging(const Context & context)
{
    bool ret = false;
    auto current_progress_value = current_progress.load();
    if (current_progress_value != last_progress_on_check)
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
    last_progress_on_check = current_progress_value;
    return ret;
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

BlockIO MPPTask::prepare(const mpp::DispatchTaskRequest & task_request)
{
    auto start_time = Clock::now();
    dag_req = std::make_unique<tipb::DAGRequest>();
    if (!dag_req->ParseFromString(task_request.encoded_plan()))
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Invalid encoded plan: " + task_request.encoded_plan(), Errors::Coprocessor::BadRequest);
    }
    std::unordered_map<RegionID, RegionInfo> regions;
    for (auto & r : task_request.regions())
    {
        auto res = regions.emplace(r.region_id(),
            RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(),
                CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
        if (!res.second)
            throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": contain duplicate region " + std::to_string(r.region_id()),
                Errors::Coprocessor::BadRequest);
    }
    // set schema ver and start ts.
    auto schema_ver = task_request.schema_ver();
    auto start_ts = task_request.meta().start_ts();

    context.setSetting("read_tso", start_ts);
    context.setSetting("schema_version", schema_ver);
    context.setSetting("mpp_task_timeout", task_request.timeout());
    context.getTimezoneInfo().resetByDAGRequest(*dag_req);
    context.setProgressCallback([this](const Progress & progress) { this->updateProgress(progress); });

    // register task.
    TMTContext & tmt_context = context.getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_DEBUG(log, "begin to register the task " << id.toString());
    if (!task_manager->registerTask(shared_from_this()))
    {
        throw TiFlashException(std::string(__PRETTY_FUNCTION__) + ": Failed to register MPP Task", Errors::Coprocessor::BadRequest);
    }


    dag_context = std::make_unique<DAGContext>(*dag_req, task_request.meta());
    context.setDAGContext(dag_context.get());

    DAGQuerySource dag(context, regions, *dag_req, true);

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
        registerTunnel(MPPTaskId{task_meta.start_ts(), task_meta.task_id()}, tunnel);
        tunnel_set->tunnels.emplace_back(tunnel);
    }
    // read index , this may take a long time.
    BlockIO streams = executeQuery(dag, context, false, QueryProcessingStage::Complete);

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
    streams.out = std::make_shared<DAGBlockOutputStream>(streams.in->getHeader(), std::move(response_writer));
    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context->compile_time_ns = compile_time_ns;
    return streams;
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
void MPPTask::runImpl(BlockIO io, MemoryTracker * memory_tracker)
{
    if (status != INITIALIZING)
    {
        LOG_WARNING(log, "task in " + taskStatusToString(status) + " state, skip running");
        return;
    }
    current_memory_tracker = memory_tracker;
    Stopwatch stopwatch;
    LOG_INFO(log, "task starts running");
    status = RUNNING;
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
    LOG_INFO(log, "task ends, time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
    auto process_info = context.getProcessListElement()->getInfo();
    auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
    GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_memory_usage, type_dispatch_mpp_task).Observe(peak_memory);
    unregisterTask();
    status = FINISHED;
}

bool MPPTask::isTaskHanging()
{
    if (status == RUNNING)
        return task_progress.isTaskHanging(context);
    return false;
}

void MPPTask::cancel()
{
    if (status == FINISHED || status == CANCELLED)
        return;
    LOG_WARNING(log, "Begin cancel task: " + id.toString());
    /// step 1. cancel query streams
    status = CANCELLED;
    auto process_list_element = context.getProcessListElement();
    if (process_list_element != nullptr && !process_list_element->streamsAreReleased())
    {
        BlockInputStreamPtr input_stream;
        BlockOutputStreamPtr output_stream;
        if (process_list_element->tryGetQueryStreams(input_stream, output_stream))
        {
            IProfilingBlockInputStream * input_stream_casted;
            if (input_stream && (input_stream_casted = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get())))
            {
                input_stream_casted->cancel(true);
            }
        }
    }
    /// step 2. write Error msg to tunnels
    writeErrToAllTunnel("MPP Task canceled because it seems hangs");
    LOG_WARNING(log, "Finish cancel task: " + id.toString());
}

// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    try
    {
        Stopwatch stopwatch;
        MPPTaskPtr task = std::make_shared<MPPTask>(task_request.meta(), context);
        auto stream = task->prepare(task_request);
        task->run(stream);
        LOG_INFO(log, "processing dispatch is over; the time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms");
    }
    catch (Exception & e)
    {
        mpp::Error error;
        LOG_ERROR(log, "dispatch task meet error : " << e.displayText());
        error.set_msg(e.displayText());
        response->set_allocated_error(&error);
    }
    catch (std::exception & e)
    {
        mpp::Error error;
        LOG_ERROR(log, "dispatch task meet error : " << e.what());
        error.set_msg(e.what());
        response->set_allocated_error(&error);
    }
    catch (...)
    {
        mpp::Error error;
        LOG_ERROR(log, "dispatch task meet fatal error");
        error.set_msg("fatal error");
        response->set_allocated_error(&error);
    }
    return grpc::Status::OK;
}

MPPTaskManager::MPPTaskManager(BackgroundProcessingPool & background_pool_)
    : log(&Logger::get("TaskManager")), background_pool(background_pool_)
{
    handle = background_pool.addTask(
        [&, this] {
            bool has_hanging_query = false;
            try
            {
                /// get a snapshot of current queries
                auto current_query = this->getCurrentQueries();
                for (auto query_id : current_query)
                {
                    /// get a snapshot of current tasks
                    auto current_tasks = this->getCurrentTasksForQuery(query_id);
                    bool has_hanging_task = false;
                    for (auto & task : current_tasks)
                    {
                        if (task->isTaskHanging())
                        {
                            has_hanging_task = true;
                            break;
                        }
                    }
                    if (has_hanging_task)
                    {
                        has_hanging_query = true;
                        this->cancelMPPQuery(query_id);
                    }
                }
            }
            catch (const Exception & e)
            {
                LOG_ERROR(log, "MPPTaskMonitor failed by " << e.displayText() << " \n stack : " << e.getStackTrace().toString());
            }
            catch (const Poco::Exception & e)
            {
                LOG_ERROR(log, "MPPTaskMonitor failed by " << e.displayText());
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(log, "MPPTaskMonitor failed by " << e.what());
            }
            return has_hanging_query;
        },
        false);
}

} // namespace DB
