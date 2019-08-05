#include <Flash/Coprocessor/CoprocessorHandler.h>

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Interpreters/InterpreterDAG.h>
#include <Interpreters/SQLQuerySource.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("CoprocessorHandler"))
{}

void CoprocessorHandler::execute()
{
    switch (cop_request->tp())
    {
        case COP_REQ_TYPE_DAG:
        {
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
            tipb::SelectResponse dag_response;
            DAGDriver driver(cop_context.db_context, dag_request, cop_context.kv_context.region_id(),
                cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(), dag_response);
            driver.execute();
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle DAG request done");
            cop_response->set_data(dag_response.SerializeAsString());
            break;
        }
        case COP_REQ_TYPE_ANALYZE:
        case COP_REQ_TYPE_CHECKSUM:
        default:
            throw Exception(
                "Coprocessor request type " + std::to_string(cop_request->tp()) + " is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }
}

} // namespace DB
