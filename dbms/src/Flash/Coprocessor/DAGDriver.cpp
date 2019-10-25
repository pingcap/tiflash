#include <Flash/Coprocessor/DAGDriver.h>

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

DAGDriver::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, RegionID region_id_, UInt64 region_version_,
    UInt64 region_conf_version_, std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> && key_ranges_,
    tipb::SelectResponse & dag_response_, bool internal_)
    : context(context_),
      dag_request(dag_request_),
      region_id(region_id_),
      region_version(region_version_),
      region_conf_version(region_conf_version_),
      key_ranges(std::move(key_ranges_)),
      dag_response(dag_response_),
      internal(internal_),
      log(&Logger::get("DAGDriver"))
{}

void DAGDriver::execute()
try
{
    context.setSetting("read_tso", UInt64(dag_request.start_ts()));

    DAGContext dag_context(dag_request.executors_size());
    DAGQuerySource dag(context, dag_context, region_id, region_version, region_conf_version, key_ranges, dag_request);
    BlockIO streams;

    String planner = context.getSettings().dag_planner;
    if (planner == "sql")
    {
        DAGStringConverter converter(context, dag_request);
        String query = converter.buildSqlString();
        if (!query.empty())
            streams = executeQuery(query, context, internal, QueryProcessingStage::Complete);
    }
    else if (planner == "optree")
    {
        streams = executeQuery(dag, context, internal, QueryProcessingStage::Complete);
    }
    else
    {
        throw Exception("Unknown DAG planner type " + planner, ErrorCodes::LOGICAL_ERROR);
    }

    if (!streams.in || streams.out)
        // Only query is allowed, so streams.in must not be null and streams.out must be null
        throw Exception("DAG is not query.", ErrorCodes::LOGICAL_ERROR);

    BlockOutputStreamPtr dag_output_stream = std::make_shared<DAGBlockOutputStream>(dag_response,
        context.getSettings().dag_records_per_chunk,
        dag_request.encode_type(),
        dag.getResultFieldTypes(),
        streams.in->getHeader());
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
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": Exception: " << e.getStackTrace().toString());
    recordError(e.code(), e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
    recordError(ErrorCodes::UNKNOWN_EXCEPTION, e.what());
}

void DAGDriver::recordError(Int32 err_code, const String & err_msg)
{
    dag_response.Clear();
    tipb::Error * error = dag_response.mutable_error();
    error->set_code(err_code);
    error->set_msg(err_msg);
}

} // namespace DB
