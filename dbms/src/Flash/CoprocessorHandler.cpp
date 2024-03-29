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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/ServiceUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/SchemaSyncer.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> genCopKeyRange(
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

CoprocessorContext::CoprocessorContext(
    Context & db_context_,
    const kvrpcpb::Context & kv_context_,
    const grpc::ServerContext & grpc_server_context_)
    : db_context(db_context_)
    , kv_context(kv_context_)
    , grpc_server_context(grpc_server_context_)
{}

template <>
CoprocessorHandler<false>::CoprocessorHandler(
    CoprocessorContext & cop_context_,
    const coprocessor::Request * cop_request_,
    coprocessor::Response * cop_response_,
    const String & identifier)
    : cop_context(cop_context_)
    , cop_request(cop_request_)
    , cop_response(cop_response_)
    , resource_group_name(cop_request->context().resource_control_context().resource_group_name())
    , log(Logger::get(identifier))
{}

template <>
CoprocessorHandler<true>::CoprocessorHandler(
    CoprocessorContext & cop_context_,
    const coprocessor::Request * cop_request_,
    grpc::ServerWriter<coprocessor::Response> * cop_writer_,
    const String & identifier)
    : cop_context(cop_context_)
    , cop_request(cop_request_)
    , cop_writer(cop_writer_)
    , resource_group_name(cop_request->context().resource_control_context().resource_group_name())
    , log(Logger::get(identifier))
{}

template <bool is_stream>
grpc::Status CoprocessorHandler<is_stream>::execute()
{
    Stopwatch watch;
    if constexpr (is_stream)
        SCOPE_EXIT({
            GET_METRIC(tiflash_coprocessor_request_handle_seconds, type_cop_stream).Observe(watch.elapsedSeconds());
        });
    else
        SCOPE_EXIT(
            { GET_METRIC(tiflash_coprocessor_request_handle_seconds, type_cop).Observe(watch.elapsedSeconds()); });

    try
    {
        RUNTIME_CHECK_MSG(
            !cop_context.db_context.getSharedContextDisagg()->isDisaggregatedComputeMode(),
            "cannot run cop or batchCop request on tiflash_compute node");

        switch (cop_request->tp())
        {
        case COP_REQ_TYPE_DAG:
        {
            if constexpr (is_stream)
            {
                GET_METRIC(tiflash_coprocessor_request_count, type_cop_stream_executing).Increment();
                GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_stream_executing).Increment();
                SCOPE_EXIT(
                    { GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_stream_executing).Decrement(); });
            }
            else
            {
                GET_METRIC(tiflash_coprocessor_request_count, type_cop_executing).Increment();
                GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_executing).Increment();
                SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop_executing).Decrement(); });
            }

            tipb::DAGRequest dag_request = getDAGRequestFromStringWithRetry(cop_request->data());
            LOG_DEBUG(log, "Handling DAG request: {}", dag_request.DebugString());
            if (dag_request.has_is_rpn_expr() && dag_request.is_rpn_expr())
                throw TiFlashException(
                    "DAG request with rpn expression is not supported in TiFlash",
                    Errors::Coprocessor::Unimplemented);
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
                    genCopKeyRange(cop_request->ranges()),
                    &bypass_lock_ts));

            DAGRequestKind kind;
            if constexpr (is_stream)
                kind = DAGRequestKind::CopStream;
            else
                kind = DAGRequestKind::Cop;

            DAGContext dag_context(
                dag_request,
                std::move(tables_regions_info),
                RequestUtils::deriveKeyspaceID(cop_request->context()),
                cop_context.db_context.getClientInfo().current_address.toString(),
                kind,
                resource_group_name,
                cop_request->connection_id(),
                cop_request->connection_alias(),
                Logger::get(log->identifier()));
            cop_context.db_context.setDAGContext(&dag_context);

            if constexpr (is_stream)
            {
                DAGDriver<DAGRequestKind::CopStream> driver(
                    cop_context.db_context,
                    cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(),
                    cop_request->schema_ver(),
                    cop_writer);
                driver.execute();
            }
            else
            {
                tipb::SelectResponse dag_response;
                DAGDriver<DAGRequestKind::Cop> driver(
                    cop_context.db_context,
                    cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(),
                    cop_request->schema_ver(),
                    &dag_response);
                driver.execute();
                cop_response->set_data(dag_response.SerializeAsString());
            }
            LOG_DEBUG(log, "Handle DAG request done");
            break;
        }
        case COP_REQ_TYPE_ANALYZE:
        case COP_REQ_TYPE_CHECKSUM:
        default:
            throw TiFlashException(
                "Coprocessor request type " + std::to_string(cop_request->tp()) + " is not implemented",
                Errors::Coprocessor::Unimplemented);
        }
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "{}\n{}", e.standardText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (LockException & e)
    {
        LOG_WARNING(log, "LockException: region_id={}, message: {}", cop_request->context().region_id(), e.message());
        GET_METRIC(tiflash_coprocessor_request_error, reason_meet_lock).Increment();
        if constexpr (is_stream)
        {
            coprocessor::Response response;
            response.set_allocated_locked(e.locks[0].second.release());
            cop_writer->Write(response);
        }
        else
        {
            cop_response->Clear();
            cop_response->set_allocated_locked(e.locks[0].second.release());
        }
        // return ok so client has the chance to see the LockException
        return grpc::Status::OK;
    }
    catch (const RegionException & e)
    {
        LOG_WARNING(log, "RegionException: region_id={}, message: {}", cop_request->context().region_id(), e.message());
        [[maybe_unused]] coprocessor::Response stream_response;
        coprocessor::Response * response;
        if constexpr (is_stream)
        {
            response = &stream_response;
        }
        else
        {
            cop_response->Clear();
            response = cop_response;
        }

        errorpb::Error * region_err;
        switch (e.status)
        {
        case RegionException::RegionReadStatus::OTHER:
        case RegionException::RegionReadStatus::BUCKET_EPOCH_NOT_MATCH:
        case RegionException::RegionReadStatus::FLASHBACK:
        case RegionException::RegionReadStatus::KEY_NOT_IN_REGION:
        case RegionException::RegionReadStatus::TIKV_SERVER_ISSUE:
        case RegionException::RegionReadStatus::READ_INDEX_TIMEOUT:
        case RegionException::RegionReadStatus::NOT_LEADER:
        case RegionException::RegionReadStatus::NOT_FOUND_TIKV:
        case RegionException::RegionReadStatus::NOT_FOUND:
            GET_METRIC(tiflash_coprocessor_request_error, reason_region_not_found).Increment();
            region_err = response->mutable_region_error();
            region_err->mutable_region_not_found()->set_region_id(cop_request->context().region_id());
            break;
        case RegionException::RegionReadStatus::EPOCH_NOT_MATCH:
            GET_METRIC(tiflash_coprocessor_request_error, reason_epoch_not_match).Increment();
            region_err = response->mutable_region_error();
            region_err->mutable_epoch_not_match();
            break;
        default:
            // should not happen
            break;
        }
        if constexpr (is_stream)
        {
            cop_writer->Write(stream_response);
        }
        // return ok so client has the chance to see the RegionException
        return grpc::Status::OK;
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        GET_METRIC(tiflash_coprocessor_request_error, reason_kv_client_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        GET_METRIC(tiflash_coprocessor_request_error, reason_other_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        GET_METRIC(tiflash_coprocessor_request_error, reason_other_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, "other exception");
    }
}

template <bool is_stream>
grpc::Status CoprocessorHandler<is_stream>::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    if constexpr (is_stream)
    {
        coprocessor::Response response;
        response.set_other_error(err_msg);
        cop_writer->Write(response);
    }
    else
    {
        cop_response->Clear();
        cop_response->set_other_error(err_msg);
    }
    return grpc::Status(err_code, err_msg);
}

template class CoprocessorHandler<false>;
template class CoprocessorHandler<true>;

} // namespace DB
