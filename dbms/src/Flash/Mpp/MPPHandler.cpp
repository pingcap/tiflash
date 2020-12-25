#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

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

    // register task.
    TMTContext & tmt_context = context.getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    LOG_DEBUG(log, "begin to register the task " << id.toString());
    task_manager->registerTask(shared_from_this());


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

void MPPTask::runImpl(BlockIO io)
{

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
    unregisterTask();
}

// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    Stopwatch stopwatch;
    try
    {
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

} // namespace DB
