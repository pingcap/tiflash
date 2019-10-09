#include <Flash/CoprocessorHandler.h>

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

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("CoprocessorHandler"))
{}

grpc::Status CoprocessorHandler::execute()
try
{
    switch (cop_request->tp())
    {
        case COP_REQ_TYPE_DAG:
        {
            std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
            for (auto & range : cop_request->ranges())
            {
                std::string start_key(range.start());
                DecodedTiKVKey start(std::move(start_key));
                std::string end_key(range.end());
                DecodedTiKVKey end(std::move(end_key));
                key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
            }
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
            tipb::SelectResponse dag_response;
            DAGDriver driver(cop_context.db_context, dag_request, cop_context.kv_context.region_id(),
                cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(), std::move(key_ranges),
                dag_response);
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
    return grpc::Status::OK;
}
catch (const LockException & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": LockException: " << e.getStackTrace().toString());
    cop_response->Clear();
    kvrpcpb::LockInfo * lock_info = cop_response->mutable_locked();
    lock_info->set_key(e.lock_infos[0]->key);
    lock_info->set_primary_lock(e.lock_infos[0]->primary_lock);
    lock_info->set_lock_ttl(e.lock_infos[0]->lock_ttl);
    lock_info->set_lock_version(e.lock_infos[0]->lock_version);
    // return ok so TiDB has the chance to see the LockException
    return grpc::Status::OK;
}
catch (const RegionException & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": RegionException: " << e.getStackTrace().toString());
    cop_response->Clear();
    errorpb::Error * region_err;
    switch (e.status)
    {
        case RegionTable::RegionReadStatus::NOT_FOUND:
        case RegionTable::RegionReadStatus::PENDING_REMOVE:
            region_err = cop_response->mutable_region_error();
            region_err->mutable_region_not_found()->set_region_id(cop_request->context().region_id());
            break;
        case RegionTable::RegionReadStatus::VERSION_ERROR:
            region_err = cop_response->mutable_region_error();
            region_err->mutable_epoch_not_match();
            break;
        default:
            // should not happen
            break;
    }
    // return ok so TiDB has the chance to see the LockException
    return grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": Exception: " << e.getStackTrace().toString());
    cop_response->Clear();
    cop_response->set_other_error(e.message());

    if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
        return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, e.message());

    // TODO: Map other DB error codes to grpc codes.

    return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
    cop_response->Clear();
    cop_response->set_other_error(e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
}

} // namespace DB
