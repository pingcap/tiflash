// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Stopwatch.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/ServiceUtils.h>
#include <Storages/IStorage.h>
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
    Context & db_context_,
    const kvrpcpb::Context & kv_context_,
    const grpc::ServerContext & grpc_server_context_)
    : db_context(db_context_)
    , kv_context(kv_context_)
    , grpc_server_context(grpc_server_context_)
{}

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_,
    const coprocessor::Request * cop_request_,
    coprocessor::Response * cop_response_)
    : cop_context(cop_context_)
    , cop_request(cop_request_)
    , cop_response(cop_response_)
    , log(&Poco::Logger::get("CoprocessorHandler"))
{}

std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> CoprocessorHandler::GenCopKeyRange(
    const ::google::protobuf::RepeatedPtrField<::coprocessor::KeyRange> & ranges)
{
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges;
    for (const auto & range : ranges)
    {
        DecodedTiKVKeyPtr start = std::make_shared<DecodedTiKVKey>(std::string(range.start()));
        DecodedTiKVKeyPtr end = std::make_shared<DecodedTiKVKey>(std::string(range.end()));
        key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
    }
    return key_ranges;
}

grpc::Status CoprocessorHandler::execute()
{
    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_request_handle_seconds, type_cop).Observe(watch.elapsedSeconds()); });

    try
    {
        switch (cop_request->tp())
        {
        case COP_REQ_TYPE_DAG:
        {
            GET_METRIC(tiflash_coprocessor_request_count, type_cop_dag).Increment();
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_dag).Increment();
            SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_dag).Decrement(); });

            tipb::DAGRequest dag_request = getDAGRequestFromStringWithRetry(cop_request->data());
            LOG_FMT_DEBUG(log, "Handling DAG request: {}", dag_request.DebugString());
            if (dag_request.has_is_rpn_expr() && dag_request.is_rpn_expr())
                throw TiFlashException(
                    "DAG request with rpn expression is not supported in TiFlash",
                    Errors::Coprocessor::Unimplemented);
            tipb::SelectResponse dag_response;
            TablesRegionsInfo tables_regions_info(true);
            auto & table_regions_info = tables_regions_info.getSingleTableRegions();

            const std::unordered_set<UInt64> bypass_lock_ts(
                cop_context.kv_context.resolved_locks().begin(),
                cop_context.kv_context.resolved_locks().end());
            table_regions_info.local_regions.emplace(
                cop_context.kv_context.region_id(),
                RegionInfo(
                    cop_context.kv_context.region_id(),
                    cop_context.kv_context.region_epoch().version(),
                    cop_context.kv_context.region_epoch().conf_ver(),
                    GenCopKeyRange(cop_request->ranges()),
                    &bypass_lock_ts));

            DAGContext dag_context(dag_request);
            dag_context.tables_regions_info = std::move(tables_regions_info);
            dag_context.log = Logger::get("CoprocessorHandler");
            dag_context.tidb_host = cop_context.db_context.getClientInfo().current_address.toString();
            cop_context.db_context.setDAGContext(&dag_context);

            DAGDriver driver(cop_context.db_context, cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(), &dag_response);
            driver.execute();
            cop_response->set_data(dag_response.SerializeAsString());
            LOG_FMT_DEBUG(log, "Handle DAG request done");
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
        LOG_FMT_ERROR(log, "{}\n{}", e.standardText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (LockException & e)
    {
        LOG_FMT_WARNING(log, "LockException: region {}, message: {}", cop_request->context().region_id(), e.message());
        cop_response->Clear();
        GET_METRIC(tiflash_coprocessor_request_error, reason_meet_lock).Increment();
        cop_response->set_allocated_locked(e.lock_info.release());
        // return ok so TiDB has the chance to see the LockException
        return grpc::Status::OK;
    }
    catch (const RegionException & e)
    {
        LOG_FMT_WARNING(log, "RegionException: region {}, message: {}", cop_request->context().region_id(), e.message());
        cop_response->Clear();
        errorpb::Error * region_err;
        switch (e.status)
        {
        case RegionException::RegionReadStatus::NOT_FOUND:
            GET_METRIC(tiflash_coprocessor_request_error, reason_region_not_found).Increment();
            region_err = cop_response->mutable_region_error();
            region_err->mutable_region_not_found()->set_region_id(cop_request->context().region_id());
            break;
        case RegionException::RegionReadStatus::EPOCH_NOT_MATCH:
            GET_METRIC(tiflash_coprocessor_request_error, reason_epoch_not_match).Increment();
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
        LOG_FMT_ERROR(log, "KV Client Exception: {}", e.message());
        GET_METRIC(tiflash_coprocessor_request_error, reason_kv_client_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const Exception & e)
    {
        LOG_FMT_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const std::exception & e)
    {
        LOG_FMT_ERROR(log, "std exception: {}", e.what());
        GET_METRIC(tiflash_coprocessor_request_error, reason_other_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_FMT_ERROR(log, "other exception");
        GET_METRIC(tiflash_coprocessor_request_error, reason_other_error).Increment();
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
