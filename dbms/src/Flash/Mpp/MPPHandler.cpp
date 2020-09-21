#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

grpc::Status MPPHandler::execute(mpp::DispatchTaskResponse * response)
{
    try
    {
        tipb::DAGRequest dag_req;
        dag_req.ParseFromString(task_request.encoded_plan());
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
        auto start_ts = task_request.meta().query_ts();

        context.setSetting("read_tso", start_ts);
        context.setSetting("schema_version", schema_ver);
        context.getTimezoneInfo().resetByDAGRequest(dag_req);

        // register task.
        TMTContext & tmt_context = context.getTMTContext();
        auto task_manager = tmt_context.getMPPTaskManager();
        // TODO: tunnel should be added to mpp task.
        MPPTaskPtr task = std::make_shared<MPPTask>(task_request.meta());
        task_manager->registerTask(task);

        DAGContext dag_context;
        DAGQuerySource dag(context, dag_context, regions, dag_req, true, task);

        BlockIO streams = executeQuery(dag, context, false, QueryProcessingStage::Complete);
        // construct writer
        MPPTunnelSetPtr tunnel_set = std::make_shared<MPPTunnelSet>();
        const auto & exchangeServer = dag_req.root_executor().exchange_server();
        for (int i = 0; i < exchangeServer.encoded_task_meta_size(); i++)
        {
            mpp::TaskMeta meta;
            meta.ParseFromString(exchangeServer.encoded_task_meta(i));
            MPPTunnelPtr tunnel = std::make_shared<MPPTunnel>();
            task->registerTunnel(MPPTaskId{meta.query_ts(), meta.task_id()}, tunnel);
            tunnel_set->tunnels.emplace_back(tunnel);
        }
        StreamingDAGResponseWriter<MPPTunnelSetPtr> response_writer(tunnel_set, context.getSettings().dag_records_per_chunk,
            dag.getEncodeType(), dag.getResultFieldTypes(), dag_context, false, true);
        streams.out = std::make_shared<DAGBlockOutputStream>(streams.in->getHeader(), response_writer);

        task->run(streams);
    }
    catch (Exception & e)
    {
        error.set_msg(e.displayText());
        response->set_allocated_error(&error);
    }
    catch (std::exception & e)
    {
        error.set_msg(e.what());
        response->set_allocated_error(&error);
    }
    return grpc::Status::OK;
}

} // namespace DB