#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/StreamingDAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <pingcap/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

DAGDriver::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const std::vector<RegionInfo> & regions_,
    UInt64 start_ts, UInt64 schema_ver, std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> && key_ranges_,
    tipb::SelectResponse & dag_response_, bool internal_)
    : context(context_),
      dag_request(dag_request_),
      regions(regions_),
      key_ranges(std::move(key_ranges_)),
      dag_response(dag_response_),
      internal(internal_),
      log(&Logger::get("DAGDriver"))
{
    context.setSetting("read_tso", start_ts);
    if (schema_ver)
        // schema_ver being 0 means TiDB/TiSpark hasn't specified schema version.
        context.setSetting("schema_version", schema_ver);
}

void DAGDriver::execute()
try
{
    DAGContext dag_context(dag_request.executors_size());
    DAGQuerySource dag(context, dag_context, regions, key_ranges, dag_request);

    BlockIO streams = executeQuery(dag, context, internal, QueryProcessingStage::Complete);
    if (!streams.in || streams.out)
        // Only query is allowed, so streams.in must not be null and streams.out must be null
        throw Exception("DAG is not query.", ErrorCodes::LOGICAL_ERROR);

    BlockOutputStreamPtr dag_output_stream = std::make_shared<DAGBlockOutputStream>(
        dag_response, context.getSettings().dag_records_per_chunk, dag.getEncodeType(), dag.getResultFieldTypes(), streams.in->getHeader());
    copyData(*streams.in, *dag_output_stream);

    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streams.in.get()))
    {
        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": dag request without encode cost: "
                                           << p_stream->getProfileInfo().execution_time/(double)1000000000 << " seconds.");
    }
    if (!dag_request.has_collect_execution_summaries() || !dag_request.collect_execution_summaries())
        return;
    // add ExecutorExecutionSummary info
    for (auto & p_streams : dag_context.profile_streams_list)
    {
        auto * executeSummary = dag_response.add_execution_summaries();
        UInt64 time_processed_ns = 0;
        UInt64 num_produced_rows = 0;
        UInt64 num_iterations = 0;
        for (auto & streamPtr : p_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streamPtr.get()))
            {
                time_processed_ns = std::max(time_processed_ns, p_stream->getProfileInfo().execution_time);
                num_produced_rows += p_stream->getProfileInfo().rows;
                num_iterations += p_stream->getProfileInfo().blocks;
            }
        }
        executeSummary->set_time_processed_ns(time_processed_ns);
        executeSummary->set_num_produced_rows(num_produced_rows);
        executeSummary->set_num_iterations(num_iterations);
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

void DAGDriver::batchExecute(::grpc::ServerWriter< ::coprocessor::BatchResponse>* writer)
try
{
    DAGContext dag_context(dag_request.executors_size());
    DAGQuerySource dag(context, dag_context, regions, key_ranges, dag_request);

    BlockIO streams = executeQuery(dag, context, internal, QueryProcessingStage::Complete);
    if (!streams.in || streams.out)
        // Only query is allowed, so streams.in must not be null and streams.out must be null
        throw Exception("DAG is not query.", ErrorCodes::LOGICAL_ERROR);

    BlockOutputStreamPtr dag_output_stream = std::make_shared<StreamingDAGBlockOutputStream>(
            writer, context.getSettings().dag_records_per_chunk, dag.getEncodeType(), dag.getResultFieldTypes(), streams.in->getHeader());
    copyData(*streams.in, *dag_output_stream);

    if (!dag_request.has_collect_execution_summaries() || !dag_request.collect_execution_summaries())
        return;
    // add ExecutorExecutionSummary info
    for (auto & p_streams : dag_context.profile_streams_list)
    {
        auto * executeSummary = dag_response.add_execution_summaries();
        UInt64 time_processed_ns = 0;
        UInt64 num_produced_rows = 0;
        UInt64 num_iterations = 0;
        for (auto & streamPtr : p_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streamPtr.get()))
            {
                time_processed_ns = std::max(time_processed_ns, p_stream->getProfileInfo().execution_time);
                num_produced_rows += p_stream->getProfileInfo().rows;
                num_iterations += p_stream->getProfileInfo().blocks;
            }
        }
        executeSummary->set_time_processed_ns(time_processed_ns);
        executeSummary->set_num_produced_rows(num_produced_rows);
        executeSummary->set_num_iterations(num_iterations);
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

void DAGDriver::recordError(Int32 err_code, const String & err_msg)
{
    dag_response.Clear();
    tipb::Error * error = dag_response.mutable_error();
    error->set_code(err_code);
    error->set_msg(err_msg);
}

} // namespace DB
