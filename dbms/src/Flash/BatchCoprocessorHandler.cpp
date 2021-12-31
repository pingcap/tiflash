#include <Common/TiFlashMetrics.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

BatchCoprocessorHandler::BatchCoprocessorHandler(
    CoprocessorContext & cop_context_,
    const coprocessor::BatchRequest * cop_request_,
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_)
    : CoprocessorHandler(cop_context_, nullptr, nullptr)
    , cop_request(cop_request_)
    , writer(writer_)
{
    log = (&Poco::Logger::get("BatchCoprocessorHandler"));
}

grpc::Status BatchCoprocessorHandler::execute()
{
    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_request_handle_seconds, type_super_batch).Observe(watch.elapsedSeconds()); });

    try
    {
        switch (cop_request->tp())
        {
        case COP_REQ_TYPE_DAG:
        {
            GET_METRIC(tiflash_coprocessor_request_count, type_super_batch_cop_dag).Increment();
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch_cop_dag).Increment();
            SCOPE_EXIT(
                { GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch_cop_dag).Decrement(); });

            auto dag_request = getDAGRequestFromStringWithRetry(cop_request->data());
            RegionInfoMap regions;
            RegionInfoList retry_regions;
            for (auto & r : cop_request->regions())
            {
                auto res = regions.emplace(r.region_id(),
                                           RegionInfo(
                                               r.region_id(),
                                               r.region_epoch().version(),
                                               r.region_epoch().conf_ver(),
                                               GenCopKeyRange(r.ranges()),
                                               nullptr));
                if (!res.second)
                {
                    retry_regions.emplace_back(RegionInfo(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver(), CoprocessorHandler::GenCopKeyRange(r.ranges()), nullptr));
                }
            }
            LOG_FMT_DEBUG(
                log,
                "{}: Handling {} regions in DAG request: {}",
                __PRETTY_FUNCTION__,
                regions.size(),
                dag_request.DebugString());

            DAGContext dag_context(dag_request);
            dag_context.is_batch_cop = true;
            dag_context.regions_for_local_read = std::move(regions);
            dag_context.regions_for_remote_read = std::move(retry_regions);
            dag_context.log = std::make_shared<LogWithPrefix>(log, "");
            dag_context.tidb_host = cop_context.db_context.getClientInfo().current_address.toString();
            cop_context.db_context.setDAGContext(&dag_context);

            DAGDriver<true> driver(cop_context.db_context, cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(), writer);
            // batch execution;
            driver.execute();
            LOG_FMT_DEBUG(log, "{}: Handle DAG request done", __PRETTY_FUNCTION__);
            break;
        }
        case COP_REQ_TYPE_ANALYZE:
        case COP_REQ_TYPE_CHECKSUM:
        default:
            throw TiFlashException("Coprocessor request type " + std::to_string(cop_request->tp()) + " is not implemented",
                                   Errors::Coprocessor::Unimplemented);
        }
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_FMT_ERROR(log, "{}: TiFlash Exception: {}\n{}", __PRETTY_FUNCTION__, e.displayText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_FMT_ERROR(log, "{}: DB Exception: {}\n{}", __PRETTY_FUNCTION__, e.message(), e.getStackTrace().toString());
        return recordError(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_FMT_ERROR(log, "{}: KV Client Exception: {}", __PRETTY_FUNCTION__, e.message());
        return recordError(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_FMT_ERROR(log, "{}: std exception: {}", __PRETTY_FUNCTION__, e.what());
        return recordError(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_FMT_ERROR(log, "{}: other exception", __PRETTY_FUNCTION__);
        return recordError(grpc::StatusCode::INTERNAL, "other exception");
    }
}

grpc::Status BatchCoprocessorHandler::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    err_response.set_other_error(err_msg);
    writer->Write(err_response);

    return grpc::Status(err_code, err_msg);
}

} // namespace DB
