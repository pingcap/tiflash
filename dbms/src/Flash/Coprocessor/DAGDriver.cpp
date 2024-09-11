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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Coprocessor/UnaryDAGResponseWriter.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <pingcap/Exception.h>

namespace DB
{
namespace FailPoints
{
extern const char cop_send_failure[];
extern const char random_cop_send_failure_failpoint[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

template <DAGRequestKind kind>
const tipb::DAGRequest & DAGDriver<kind>::dagRequest() const
{
    return *context.getDAGContext()->dag_request;
}

template <>
DAGDriver<DAGRequestKind::Cop>::DAGDriver(
    Context & context_,
    UInt64 start_ts,
    UInt64 schema_ver,
    tipb::SelectResponse * cop_response_,
    bool internal_)
    : context(context_)
    , cop_response(cop_response_)
    , internal(internal_)
    , log(Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
    context.getTimezoneInfo().resetByDAGRequest(dagRequest());
}

template <>
DAGDriver<DAGRequestKind::CopStream>::DAGDriver(
    Context & context_,
    UInt64 start_ts,
    UInt64 schema_ver,
    grpc::ServerWriter<::coprocessor::Response> * cop_writer_,
    bool internal_)
    : context(context_)
    , cop_writer(cop_writer_)
    , internal(internal_)
    , log(Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
    context.getTimezoneInfo().resetByDAGRequest(dagRequest());
}

template <>
DAGDriver<DAGRequestKind::BatchCop>::DAGDriver(
    Context & context_,
    UInt64 start_ts,
    UInt64 schema_ver,
    grpc::ServerWriter<coprocessor::BatchResponse> * batch_cop_writer_,
    bool internal_)
    : context(context_)
    , batch_cop_writer(batch_cop_writer_)
    , internal(internal_)
    , log(Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
    context.getTimezoneInfo().resetByDAGRequest(dagRequest());
}

template <DAGRequestKind Kind>
void DAGDriver<Kind>::execute()
try
{
    auto start_time = Clock::now();
    DAGContext & dag_context = *context.getDAGContext();
    const auto & resource_group = dag_context.getResourceGroupName();

    auto query_executor = queryExecute(context, internal);
    if (!query_executor)
        // Only query is allowed, so query_executor must not be null
        throw TiFlashException("DAG is not query.", Errors::Coprocessor::Internal);

    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context.compile_time_ns = compile_time_ns;
    LOG_DEBUG(log, "Compile dag request cost {} ms", compile_time_ns / 1000000);

    BlockOutputStreamPtr dag_output_stream = nullptr;
    auto update_ru_statistics = [&]() -> RUConsumption {
        RUConsumption ru_info{
            .cpu_time_ns = query_executor->collectCPUTimeNs(),
            .read_bytes = dag_context.getReadBytes()};
        ru_info.cpu_ru = cpuTimeToRU(ru_info.cpu_time_ns);
        ru_info.read_ru = bytesToRU(ru_info.read_bytes);
        return ru_info;
    };
    if constexpr (Kind == DAGRequestKind::Cop)
    {
        auto response_writer = std::make_unique<UnaryDAGResponseWriter>(
            cop_response,
            context.getSettingsRef().dag_records_per_chunk,
            dag_context);
        response_writer->prepare(query_executor->getSampleBlock());
        query_executor
            ->execute([&response_writer](const Block & block) {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::cop_send_failure);
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_cop_send_failure_failpoint);
                response_writer->write(block);
            })
            .verify();
        response_writer->flush();

        auto ru_info = update_ru_statistics();
        LOG_INFO(log, "cop finish with request unit: cpu={} read={}", ru_info.cpu_ru, ru_info.read_ru);
        GET_RESOURCE_GROUP_METRIC(tiflash_compute_request_unit, type_cop, resource_group)
            .Increment(ru_info.cpu_ru + ru_info.read_ru);
        if (dag_context.collect_execution_summaries)
        {
            ExecutorStatisticsCollector statistics_collector(log->identifier());
            statistics_collector.setLocalRUConsumption(ru_info);
            statistics_collector.initialize(&dag_context);
            statistics_collector.fillExecuteSummaries(*cop_response);
        }
    }
    else if constexpr (Kind == DAGRequestKind::CopStream)
    {
        auto streaming_writer = std::make_shared<CopStreamWriter>(cop_writer);
        TiDB::TiDBCollators collators;
        auto response_writer = std::make_unique<StreamingDAGResponseWriter<CopStreamWriterPtr>>(
            streaming_writer,
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            dag_context);
        response_writer->prepare(query_executor->getSampleBlock());
        query_executor
            ->execute([&response_writer](const Block & block) {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::cop_send_failure);
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_cop_send_failure_failpoint);
                response_writer->write(block);
            })
            .verify();
        response_writer->flush();

        tipb::SelectResponse last_response;
        bool need_send = false;
        auto ru_info = update_ru_statistics();
        LOG_INFO(log, "cop stream finish with request unit: cpu={} read={}", ru_info.cpu_ru, ru_info.read_ru);
        GET_RESOURCE_GROUP_METRIC(tiflash_compute_request_unit, type_cop_stream, resource_group)
            .Increment(ru_info.cpu_ru + ru_info.read_ru);
        if (dag_context.collect_execution_summaries)
        {
            ExecutorStatisticsCollector statistics_collector(log->identifier());
            statistics_collector.setLocalRUConsumption(ru_info);
            statistics_collector.initialize(&dag_context);
            statistics_collector.fillExecuteSummaries(last_response);
            need_send = true;
        }
        if (dag_context.getWarningCount() > 0)
        {
            dag_context.fillWarnings(last_response);
            need_send = true;
        }
        if (need_send)
            streaming_writer->write(last_response);
    }
    else if constexpr (Kind == DAGRequestKind::BatchCop)
    {
        if (!dag_context.retry_regions.empty())
        {
            coprocessor::BatchResponse response;
            for (const auto & region : dag_context.retry_regions)
            {
                auto * retry_region = response.add_retry_regions();
                retry_region->set_id(region.region_id);
                retry_region->mutable_region_epoch()->set_conf_ver(region.region_conf_version);
                retry_region->mutable_region_epoch()->set_version(region.region_version);
            }
            batch_cop_writer->Write(response);
        }

        auto streaming_writer = std::make_shared<BatchCopStreamWriter>(batch_cop_writer);
        TiDB::TiDBCollators collators;
        auto response_writer = std::make_unique<StreamingDAGResponseWriter<BatchCopStreamWriterPtr>>(
            streaming_writer,
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            dag_context);
        response_writer->prepare(query_executor->getSampleBlock());
        query_executor
            ->execute([&response_writer](const Block & block) {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::cop_send_failure);
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_cop_send_failure_failpoint);
                response_writer->write(block);
            })
            .verify();
        response_writer->flush();

        tipb::SelectResponse last_response;
        bool need_send = false;
        auto ru_info = update_ru_statistics();
        LOG_INFO(log, "batch cop finish with request unit: cpu={} read={}", ru_info.cpu_ru, ru_info.read_ru);
        GET_RESOURCE_GROUP_METRIC(tiflash_compute_request_unit, type_batch, resource_group)
            .Increment(ru_info.cpu_ru + ru_info.read_ru);
        if (dag_context.collect_execution_summaries)
        {
            ExecutorStatisticsCollector statistics_collector(log->identifier());
            statistics_collector.setLocalRUConsumption(ru_info);
            statistics_collector.initialize(&dag_context);
            statistics_collector.fillExecuteSummaries(last_response);
            need_send = true;
        }
        if (dag_context.getWarningCount() > 0)
        {
            dag_context.fillWarnings(last_response);
            need_send = true;
        }
        if (need_send)
            streaming_writer->write(last_response);
    }

    if (auto throughput = dag_context.getTableScanThroughput(); throughput.first)
        GET_METRIC(tiflash_storage_logical_throughput_bytes).Observe(throughput.second);

    if (context.getProcessListElement())
    {
        auto process_info = context.getProcessListElement()->getInfo();
        auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
        if constexpr (Kind == DAGRequestKind::Cop)
        {
            GET_METRIC(tiflash_coprocessor_request_memory_usage, type_cop).Observe(peak_memory);
        }
        else if constexpr (Kind == DAGRequestKind::CopStream)
        {
            GET_METRIC(tiflash_coprocessor_request_memory_usage, type_cop_stream).Observe(peak_memory);
        }
        else if constexpr (Kind == DAGRequestKind::BatchCop)
        {
            GET_METRIC(tiflash_coprocessor_request_memory_usage, type_batch).Observe(peak_memory);
        }
    }

    auto runtime_statistics = query_executor->getRuntimeStatistics();
    LOG_DEBUG(
        log,
        "dag request without encode cost: {} seconds, produce {} rows, {} bytes.",
        runtime_statistics.execution_time_ns / static_cast<double>(1000000000),
        runtime_statistics.rows,
        runtime_statistics.bytes);
}
catch (const RegionException & e)
{
    throw;
}
catch (const LockException & e)
{
    throw;
}
catch (const TiFlashException & e)
{
    LOG_ERROR(log, "{}\n{}", e.standardText(), e.getStackTrace().toString());
    recordError(grpc::StatusCode::INTERNAL, e.standardText());
}
catch (const Exception & e)
{
    LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
    recordError(e.code(), e.message());
}
catch (const pingcap::Exception & e)
{
    LOG_ERROR(log, "KV Client Exception: {}", e.message());
    recordError(e.code(), e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, "std exception: {}", e.what());
    recordError(ErrorCodes::UNKNOWN_EXCEPTION, e.what());
}
catch (...)
{
    LOG_ERROR(log, "other exception");
    recordError(ErrorCodes::UNKNOWN_EXCEPTION, "other exception");
}

template <DAGRequestKind Kind>
void DAGDriver<Kind>::recordError(Int32 err_code, const String & err_msg)
{
    if constexpr (Kind == DAGRequestKind::Cop)
    {
        cop_response->Clear();
        tipb::Error * error = cop_response->mutable_error();
        error->set_code(err_code);
        error->set_msg(err_msg);
    }
    else if constexpr (Kind == DAGRequestKind::CopStream)
    {
        tipb::SelectResponse dag_response;
        tipb::Error * error = dag_response.mutable_error();
        error->set_code(err_code);
        error->set_msg(err_msg);
        coprocessor::Response err_response;
        err_response.set_data(dag_response.SerializeAsString());
        cop_writer->Write(err_response);
    }
    else if constexpr (Kind == DAGRequestKind::BatchCop)
    {
        tipb::SelectResponse dag_response;
        tipb::Error * error = dag_response.mutable_error();
        error->set_code(err_code);
        error->set_msg(err_msg);
        coprocessor::BatchResponse err_response;
        err_response.set_data(dag_response.SerializeAsString());
        batch_cop_writer->Write(err_response);
    }
}

template class DAGDriver<DAGRequestKind::Cop>;
template class DAGDriver<DAGRequestKind::CopStream>;
template class DAGDriver<DAGRequestKind::BatchCop>;

} // namespace DB
