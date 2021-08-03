#include <Common/TiFlashMetrics.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Coprocessor/UnaryDAGResponseWriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTContext.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/LockResolver.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

template <>
DAGDriver<false>::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const RegionInfoMap & regions_,
    const RegionInfoList & retry_regions_, UInt64 start_ts, UInt64 schema_ver, tipb::SelectResponse * dag_response_, bool internal_)
    : context(context_),
      dag_request(dag_request_),
      regions(regions_),
      retry_regions(retry_regions_),
      dag_response(dag_response_),
      writer(nullptr),
      internal(internal_),
      log(&Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
    context.getTimezoneInfo().resetByDAGRequest(dag_request);
}

template <>
DAGDriver<true>::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const RegionInfoMap & regions_,
    const RegionInfoList & retry_regions_, UInt64 start_ts, UInt64 schema_ver, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_,
    bool internal_)
    : context(context_),
      dag_request(dag_request_),
      regions(regions_),
      retry_regions(retry_regions_),
      writer(writer_),
      internal(internal_),
      log(&Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
    context.getTimezoneInfo().resetByDAGRequest(dag_request);
}

template <bool batch>
void DAGDriver<batch>::execute()
try
{
    auto start_time = Clock::now();
    DAGContext dag_context(dag_request);
    context.setDAGContext(&dag_context);
    DAGQuerySource dag(context, regions, retry_regions, dag_request, batch);

    BlockIO streams = executeQuery(dag, context, internal, QueryProcessingStage::Complete);
    if (!streams.in || streams.out)
        // Only query is allowed, so streams.in must not be null and streams.out must be null
        throw TiFlashException("DAG is not query.", Errors::Coprocessor::Internal);

    auto end_time = Clock::now();
    Int64 compile_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    dag_context.compile_time_ns = compile_time_ns;
    LOG_DEBUG(log, "Compile dag request cost " << compile_time_ns / 1000000 << " ms");

    BlockOutputStreamPtr dag_output_stream = nullptr;
    if constexpr (!batch)
    {
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<UnaryDAGResponseWriter>(
            dag_response, context.getSettings().dag_records_per_chunk, dag.getEncodeType(), dag.getResultFieldTypes(), dag_context);
        dag_output_stream = std::make_shared<DAGBlockOutputStream>(streams.in->getHeader(), std::move(response_writer));
        copyData(*streams.in, *dag_output_stream);
    }
    else
    {
        if (!dag_context.retry_regions.empty())
        {
            coprocessor::BatchResponse response;
            for (auto region : dag_context.retry_regions)
            {
                auto * retry_region = response.add_retry_regions();
                retry_region->set_id(region.region_id);
                retry_region->mutable_region_epoch()->set_conf_ver(region.region_conf_version);
                retry_region->mutable_region_epoch()->set_version(region.region_version);
            }
            writer->Write(response);
        }
        auto streaming_writer = std::make_shared<StreamWriter>(writer);
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<StreamWriterPtr>>(streaming_writer,
            std::vector<Int64>(), tipb::ExchangeType::PassThrough, context.getSettings().dag_records_per_chunk, dag.getEncodeType(),
            dag.getResultFieldTypes(), dag_context);
        dag_output_stream = std::make_shared<DAGBlockOutputStream>(streams.in->getHeader(), std::move(response_writer));
        copyData(*streams.in, *dag_output_stream);
    }

    auto process_info = context.getProcessListElement()->getInfo();
    auto peak_memory = process_info.peak_memory_usage > 0 ? process_info.peak_memory_usage : 0;
    if constexpr (!batch)
    {
        GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_memory_usage, type_cop).Observe(peak_memory);
    }
    else
    {
        GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_memory_usage, type_super_batch).Observe(peak_memory);
    }

    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streams.in.get()))
    {
        LOG_DEBUG(log,
            __PRETTY_FUNCTION__ << ": dag request without encode cost: " << p_stream->getProfileInfo().execution_time / (double)1000000000
                                << " seconds, produce " << p_stream->getProfileInfo().rows << " rows, " << p_stream->getProfileInfo().bytes
                                << " bytes.");

        if constexpr (!batch)
        {
            // Under some test cases, there may be dag response whose size is bigger than INT_MAX, and GRPC can not limit it.
            // Throw exception to prevent receiver from getting wrong response.
            if (p_stream->getProfileInfo().bytes > std::numeric_limits<int>::max())
                throw TiFlashException("DAG response is too big, please check config about region size or region merge scheduler",
                    Errors::Coprocessor::Internal);
        }
    }
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
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": " << e.standardText() << "\n" << e.getStackTrace().toString());
    recordError(grpc::StatusCode::INTERNAL, e.standardText());
}
catch (const Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": DB Exception: " << e.message() << "\n" << e.getStackTrace().toString());
    recordError(e.code(), e.message());
}
catch (const pingcap::Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": KV Client Exception: " << e.message());
    recordError(e.code(), e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
    recordError(ErrorCodes::UNKNOWN_EXCEPTION, e.what());
}
catch (...)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": other exception");
    recordError(ErrorCodes::UNKNOWN_EXCEPTION, "other exception");
}

template <bool batch>
void DAGDriver<batch>::recordError(Int32 err_code, const String & err_msg)
{
    if constexpr (batch)
    {
        tipb::SelectResponse dag_response;
        tipb::Error * error = dag_response.mutable_error();
        error->set_code(err_code);
        error->set_msg(err_msg);
        coprocessor::BatchResponse err_response;
        err_response.set_data(dag_response.SerializeAsString());
        writer->Write(err_response);
    }
    else
    {
        dag_response->Clear();
        tipb::Error * error = dag_response->mutable_error();
        error->set_code(err_code);
        error->set_msg(err_msg);
    }
}

template class DAGDriver<true>;
template class DAGDriver<false>;

} // namespace DB
