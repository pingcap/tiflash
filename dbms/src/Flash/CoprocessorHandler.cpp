#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/CoprocessorHandler.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

CoprocessorContext::CoprocessorContext(
    Context & db_context_, const kvrpcpb::Context & kv_context_, const grpc::ServerContext & grpc_server_context_)
    : db_context(db_context_), kv_context(kv_context_), grpc_server_context(grpc_server_context_), metrics(db_context.getTiFlashMetrics())
{}

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("CoprocessorHandler"))
{}

grpc::Status CoprocessorHandler::execute()
{
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_handle_seconds, type_cop).Observe(watch.elapsedSeconds());
    });

    try
    {
        switch (cop_request->tp())
        {
            case COP_REQ_TYPE_DAG:
            {
                GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_count, type_cop_dag).Increment();
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
                if (dag_request.has_is_rpn_expr() && dag_request.is_rpn_expr())
                    throw Exception("DAG request with rpn expression is not supported in TiFlash", ErrorCodes::NOT_IMPLEMENTED);
                tipb::SelectResponse dag_response;
                DAGDriver driver(cop_context.db_context, dag_request, cop_context.kv_context.region_id(),
                    cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(),
                    cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(),
                    std::move(key_ranges), dag_response);
                driver.execute();
                cop_response->set_data(dag_response.SerializeAsString());
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
    catch (const LockException & e)
    {
        LOG_WARNING(
            log, __PRETTY_FUNCTION__ << ": LockException: region " << cop_request->context().region_id() << ", message: " << e.message());
        cop_response->Clear();
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_meet_lock).Increment();
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
        LOG_WARNING(
            log, __PRETTY_FUNCTION__ << ": RegionException: region " << cop_request->context().region_id() << ", message: " << e.message());
        cop_response->Clear();
        errorpb::Error * region_err;
        switch (e.status)
        {
            case RegionException::RegionReadStatus::NOT_FOUND:
            case RegionException::RegionReadStatus::PENDING_REMOVE:
                GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_region_not_found).Increment();
                region_err = cop_response->mutable_region_error();
                region_err->mutable_region_not_found()->set_region_id(cop_request->context().region_id());
                break;
            case RegionException::RegionReadStatus::VERSION_ERROR:
                GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_epoch_not_match).Increment();
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
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": KV Client Exception: " << e.message());
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_kv_client_error).Increment();
        return recordError(
            e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": DB Exception: " << e.message() << "\n" << e.getStackTrace().toString());
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(
            e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_other_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": other exception");
        GET_METRIC(cop_context.metrics, tiflash_coprocessor_request_error, reason_other_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, "other exception");
    }
}

grpc::Status CoprocessorHandler::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    cop_response->Clear();
    cop_response->set_other_error(err_msg);

    return grpc::Status(err_code, err_msg);
}

} // namespace DB
