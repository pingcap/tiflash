#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>

namespace DB
{

void DAGResponseWriter::fillTiExecutionSummary(
    tipb::ExecutorExecutionSummary * execution_summary, ExecutionSummary & current, const String & executor_id, bool delta_mode)
{
    auto & prev_stats = previous_execution_stats[executor_id];

    execution_summary->set_time_processed_ns(
        delta_mode ? current.time_processed_ns - prev_stats.time_processed_ns : current.time_processed_ns);
    execution_summary->set_num_produced_rows(
        delta_mode ? current.num_produced_rows - prev_stats.num_produced_rows : current.num_produced_rows);
    execution_summary->set_num_iterations(delta_mode ? current.num_iterations - prev_stats.num_iterations : current.num_iterations);
    execution_summary->set_concurrency(delta_mode ? current.concurrency - prev_stats.concurrency : current.concurrency);
    prev_stats = current;
    if (dag_context.return_executor_id)
        execution_summary->set_executor_id(executor_id);
}

void DAGResponseWriter::addExecuteSummaries(tipb::SelectResponse & response, bool delta_mode)
{
    /// delta_mode means when for a streaming call, return the delta execution summary
    /// because TiDB is not aware of the streaming call when it handle the execution summaries
    /// so we need to "pretend to be a unary call", can be removed if TiDB support streaming
    /// call's execution summaries directly
    if (!dag_context.collect_execution_summaries)
        return;
    /// add execution_summary for local executor
    for (auto & p : dag_context.getProfileStreamsMap())
    {
        ExecutionSummary current;
        /// part 1: local execution info
        for (auto & streamPtr : p.second.input_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streamPtr.get()))
            {
                current.time_processed_ns = std::max(current.time_processed_ns, p_stream->getProfileInfo().execution_time);
                current.num_produced_rows += p_stream->getProfileInfo().rows;
                current.num_iterations += p_stream->getProfileInfo().blocks;
            }
            current.concurrency++;
        }
        /// part 2: remote execution info
        for (auto & streamPtr : dag_context.getRemoteInputStreams())
        {
            auto coprocessor_input_stream = dynamic_cast<CoprocessorBlockInputStream *>(streamPtr.get());
            auto exchange_receiver_input_stream = dynamic_cast<ExchangeReceiverInputStream *>(streamPtr.get());
            auto remote_execution_summaries = coprocessor_input_stream != nullptr
                ? coprocessor_input_stream->getRemoteExecutionSummaries()
                : exchange_receiver_input_stream->getRemoteExecutionSummaries();
            if (remote_execution_summaries != nullptr)
            {
                bool is_streaming_call = coprocessor_input_stream != nullptr ? coprocessor_input_stream->isStreamingCall()
                                                                             : exchange_receiver_input_stream->isStreamingCall();
                ExecutionSummary remote;

                const auto & remote_execution_info = remote_execution_summaries->find(p.first);
                if (remote_execution_info != remote_execution_summaries->end())
                {
                    for (const auto & remote_execution_summary : remote_execution_info->second)
                    {
                        remote.merge(remote_execution_summary, is_streaming_call);
                    }
                    current.merge(remote, false);
                }
            }
        }
        /// part 3: for join need to add the build time
        for (auto & join_alias : dag_context.getQBIdToJoinAliasMap()[p.second.qb_id])
        {
            UInt64 process_time_for_build = 0;
            if (dag_context.getProfileStreamsMapForJoinBuildSide().find(join_alias)
                != dag_context.getProfileStreamsMapForJoinBuildSide().end())
            {
                for (auto & join_stream : dag_context.getProfileStreamsMapForJoinBuildSide()[join_alias])
                {
                    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_stream.get()))
                        process_time_for_build = std::max(process_time_for_build, p_stream->getProfileInfo().execution_time);
                }
            }
            current.time_processed_ns += process_time_for_build;
        }

        current.time_processed_ns += dag_context.compile_time_ns;
        fillTiExecutionSummary(response.add_execution_summaries(), current, p.first, delta_mode);
        /// do not have an easy and meaningful way to get the execution summary for exchange sender
        /// executor, however, TiDB requires execution summary for all the executors, so just return
        /// its child executor's execution summary
        if (dag_context.isMPPTask() && p.first == dag_context.exchange_sender_execution_summary_key)
        {
            current.concurrency = dag_context.final_concurrency;
            fillTiExecutionSummary(response.add_execution_summaries(), current, dag_context.exchange_sender_executor_id, delta_mode);
        }
    }
    /// add executionSummary for remote executor
    std::unordered_map<String, ExecutionSummary> merged_remote_execution_summaries;
    for (auto & streamPtr : dag_context.getRemoteInputStreams())
    {
        auto coprocessor_input_stream = dynamic_cast<CoprocessorBlockInputStream *>(streamPtr.get());
        auto exchange_receiver_input_stream = dynamic_cast<ExchangeReceiverInputStream *>(streamPtr.get());
        auto remote_execution_summaries = coprocessor_input_stream != nullptr
            ? coprocessor_input_stream->getRemoteExecutionSummaries()
            : exchange_receiver_input_stream->getRemoteExecutionSummaries();
        if (remote_execution_summaries != nullptr)
        {
            bool is_streaming_call = coprocessor_input_stream != nullptr ? coprocessor_input_stream->isStreamingCall()
                                                                         : exchange_receiver_input_stream->isStreamingCall();
            for (auto & p : *remote_execution_summaries)
            {
                if (local_executors.find(p.first) == local_executors.end())
                {
                    auto & current = merged_remote_execution_summaries[p.first];
                    for (const auto & remote_execution_summary : p.second)
                    {
                        current.merge(remote_execution_summary, is_streaming_call);
                    }
                }
            }
        }
    }
    for (auto & p : merged_remote_execution_summaries)
        fillTiExecutionSummary(response.add_execution_summaries(), p.second, p.first, delta_mode);
}

DAGResponseWriter::DAGResponseWriter(
    Int64 records_per_chunk_, tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_)
    : records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      result_field_types(std::move(result_field_types_)),
      dag_context(dag_context_)
{
    for (auto & p : dag_context.getProfileStreamsMap())
    {
        local_executors.insert(p.first);
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
