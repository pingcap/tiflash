#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>

namespace DB
{

void DAGResponseWriter::addExecuteSummaries(tipb::SelectResponse & response)
{
    if (!collect_execute_summary)
        return;
    // add ExecutorExecutionSummary info
    for (auto & p : dag_context.getProfileStreamsMap())
    {
        auto * executeSummary = response.add_execution_summaries();
        UInt64 time_processed_ns = 0;
        UInt64 num_produced_rows = 0;
        UInt64 num_iterations = 0;
        for (auto & streamPtr : p.second.input_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streamPtr.get()))
            {
                time_processed_ns = std::max(time_processed_ns, p_stream->getProfileInfo().execution_time);
                num_produced_rows += p_stream->getProfileInfo().rows;
                num_iterations += p_stream->getProfileInfo().blocks;
            }
        }
        for (auto & join_alias : dag_context.getQBIdToJoinAliasMap()[p.second.qb_id])
        {
            if (dag_context.getProfileStreamsMapForJoinBuildSide().find(join_alias)
                != dag_context.getProfileStreamsMapForJoinBuildSide().end())
            {
                UInt64 process_time_for_build = 0;
                for (auto & join_stream : dag_context.getProfileStreamsMapForJoinBuildSide()[join_alias])
                {
                    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_stream.get()))
                        process_time_for_build = std::max(process_time_for_build, p_stream->getProfileInfo().execution_time);
                }
                time_processed_ns += process_time_for_build;
            }
        }

        time_processed_ns += dag_context.compile_time_ns;
        auto & prev_stats = previous_execute_stats[p.first];

        executeSummary->set_time_processed_ns(time_processed_ns - std::get<0>(prev_stats));
        executeSummary->set_num_produced_rows(num_produced_rows - std::get<1>(prev_stats));
        executeSummary->set_num_iterations(num_iterations - std::get<2>(prev_stats));
        std::get<0>(prev_stats) = time_processed_ns;
        std::get<1>(prev_stats) = num_produced_rows;
        std::get<2>(prev_stats) = num_iterations;
        if (return_executor_id)
            executeSummary->set_executor_id(p.first);
    }
}

DAGResponseWriter::DAGResponseWriter(Int64 records_per_chunk_, tipb::EncodeType encode_type_,
    std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_, bool collect_execute_summary_, bool return_executor_id_)
    : records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      result_field_types(std::move(result_field_types_)),
      dag_context(dag_context_),
      collect_execute_summary(collect_execute_summary_),
      return_executor_id(return_executor_id_)
{
    for (auto & p : dag_context.getProfileStreamsMap())
    {
        previous_execute_stats[p.first] = std::make_tuple(0, 0, 0);
    }
    if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        records_per_chunk = -1;
    }
    if (encode_type != tipb::EncodeType::TypeCHBlock && encode_type != tipb::EncodeType::TypeChunk
        && encode_type != tipb::EncodeType::TypeDefault)
    {
        throw TiFlashException(
            "Only Default/Arrow/CHBlock encode type is supported in DAGBlockOutputStream.", Errors::Coprocessor::Unimplemented);
    }
}

} // namespace DB
