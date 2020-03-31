#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

BatchCoprocessorHandler::BatchCoprocessorHandler(CoprocessorContext & cop_context_, const coprocessor::BatchRequest * cop_request_,
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_)
    : cop_context(cop_context_), cop_request(cop_request_), writer(writer_), log(&Logger::get("BatchCoprocessorHandler"))
{}

grpc::Status BatchCoprocessorHandler::execute()
try
{
    switch (cop_request->tp())
    {
        case COP_REQ_TYPE_DAG:
        {
            std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
            std::unordered_map<RegionID, RegionInfo> regions;
            for (auto & r : cop_request->regions())
            {
                std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
                for (auto & range : r.ranges())
                {
                    std::string start_key(range.start());
                    DecodedTiKVKey start(std::move(start_key));
                    std::string end_key(range.end());
                    DecodedTiKVKey end(std::move(end_key));
                    key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
                }
                regions.emplace(r.region_id(),
                    RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(), std::move(key_ranges)));
            }
            tipb::SelectResponse dag_response; // unused
            DAGDriver driver(cop_context.db_context, dag_request, regions,
                cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(),
                dag_response);
            // batch execution;
            driver.batchExecute(writer);
            if (dag_response.has_error())
            {
                err_response.set_other_error(dag_response.error().msg());
                writer->Write(err_response);
            }
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle DAG request done");
            break;
        }
        case COP_REQ_TYPE_ANALYZE:
        case COP_REQ_TYPE_CHECKSUM:
        default:
            throw Exception(
                "Coprocessor request type " + std::to_string(cop_request->tp()) + " is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }
    return grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": DB Exception: " << e.message() << "\n" << e.getStackTrace().toString());
    return recordError(e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
}
catch (const pingcap::Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": KV Client Exception: " << e.message());
    return recordError(e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
    return recordError(grpc::StatusCode::INTERNAL, e.what());
}
catch (...)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": other exception");
    return recordError(grpc::StatusCode::INTERNAL, "other exception");
}

grpc::Status BatchCoprocessorHandler::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    err_response.set_other_error(err_msg);

    return grpc::Status(err_code, err_msg);
}

} // namespace DB
