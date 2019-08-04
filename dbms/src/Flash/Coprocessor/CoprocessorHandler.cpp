#include <Flash/Coprocessor/CoprocessorHandler.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
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

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("CoprocessorHandler"))
{}

CoprocessorHandler::~CoprocessorHandler() {}

bool CoprocessorHandler::execute()
{
    switch (cop_request->tp())
    {
        case REQ_TYPE_DAG:
        {
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            tipb::SelectResponse dag_response;
            DAGDriver driver(cop_context.db_context, dag_request, cop_context.kv_context.region_id(),
                cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(), dag_response);
            if (driver.execute())
            {
                cop_response->set_data(dag_response.SerializeAsString());
                return true;
            }
            return false;
        }
        case REQ_TYPE_ANALYZE:
        case REQ_TYPE_CHECKSUM:
        default:
            LOG_ERROR(log, "Flash service Coprocessor other than dag request not implement yet");
            // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Only DAG request is supported");
            return false;
    }

    return true;
}

} // namespace DB
