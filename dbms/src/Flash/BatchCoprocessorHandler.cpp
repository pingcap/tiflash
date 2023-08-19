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

#include <Common/TiFlashMetrics.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/ServiceUtils.h>
#include <Storages/IStorage.h>
#include <Storages/Transaction/TMTContext.h>
#include <TiDB/Schema/SchemaSyncer.h>

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
    SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_request_handle_seconds, type_batch).Observe(watch.elapsedSeconds()); });

    try
    {
        switch (cop_request->tp())
        {
        case COP_REQ_TYPE_DAG:
        {
            GET_METRIC(tiflash_coprocessor_request_count, type_batch_executing).Increment();
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch_executing).Increment();
            SCOPE_EXIT(
                { GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch_executing).Decrement(); });

            auto dag_request = getDAGRequestFromStringWithRetry(cop_request->data());
            auto tables_regions_info = TablesRegionsInfo::create(cop_request->regions(), cop_request->table_regions(), cop_context.db_context.getTMTContext());
            LOG_DEBUG(
                log,
                "Handling {} regions from {} physical tables in DAG request: {}",
                tables_regions_info.regionCount(),
                tables_regions_info.tableCount(),
                dag_request.DebugString());

            DAGContext dag_context(dag_request);
            dag_context.is_batch_cop = true;
            dag_context.tables_regions_info = std::move(tables_regions_info);
            dag_context.log = Logger::get("BatchCoprocessorHandler");
            dag_context.tidb_host = cop_context.db_context.getClientInfo().current_address.toString();
            cop_context.db_context.setDAGContext(&dag_context);

            DAGDriver<true> driver(cop_context.db_context, cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(), writer);
            // batch execution;
            driver.execute();
            LOG_DEBUG(log, "Handle DAG request done");
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
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_coprocessor_request_error, reason_internal_error).Increment();
        return recordError(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        return recordError(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        return recordError(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        return recordError(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
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
