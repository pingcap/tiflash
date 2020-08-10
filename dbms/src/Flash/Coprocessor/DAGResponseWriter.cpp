#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <bool streaming>
DAGResponseWriter<streaming>::DAGResponseWriter(tipb::SelectResponse * dag_response_, StreamWriterPtr writer_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_,
    bool collect_execute_summary_, bool return_executor_id_)
    : dag_response(dag_response_),
      writer(std::move(writer_)),
      result_field_types(std::move(result_field_types_)),
      records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      current_records_num(0),
      dag_context(dag_context_),
      collect_execute_summary(collect_execute_summary_),
      return_executor_id(return_executor_id_)
{
    previous_execute_stats.resize(dag_context.getProfileStreamsMap().size(), std::make_tuple(0, 0, 0));
    if (encode_type == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeChunk)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
        records_per_chunk = -1;
    }
    else
    {
        throw TiFlashException("Only Default and Arrow encode type is supported in DAGBlockOutputStream.",
            Errors::Coprocessor::Unimplemented);
    }
    if (dag_response)
        dag_response->set_encode_type(encode_type);
}

template <bool streaming>
void DAGResponseWriter<streaming>::addExecuteSummaries(tipb::SelectResponse * response)
{
    if (!collect_execute_summary)
        return;
    // add ExecutorExecutionSummary info
    for (auto & p : dag_context.getProfileStreamsMap())
    {
        auto * executeSummary = response->add_execution_summaries();
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
        executeSummary->set_time_processed_ns(time_processed_ns);
        executeSummary->set_num_produced_rows(num_produced_rows);
        executeSummary->set_num_iterations(num_iterations);
        if (return_executor_id)
            executeSummary->set_executor_id(p.first);
    }
}

template <>
void DAGResponseWriter<false>::encodeChunkToDAGResponse()
{
    auto dag_chunk = dag_response->add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    current_records_num = 0;
}

template <>
void DAGResponseWriter<true>::encodeChunkToDAGResponse()
{
    ::coprocessor::BatchResponse resp;

    tipb::SelectResponse stream_dag_response;
    stream_dag_response.set_encode_type(encode_type);
    auto dag_chunk = stream_dag_response.add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    current_records_num = 0;
    addExecuteSummaries(&stream_dag_response);
    std::string dag_data;
    stream_dag_response.SerializeToString(&dag_data);
    resp.set_data(dag_data);

    writer->write(resp);
}

template <bool streaming>
void DAGResponseWriter<streaming>::finishWrite()
{
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
    if constexpr (!streaming)
    {
        addExecuteSummaries(dag_response);
    }
}

template <bool streaming>
void DAGResponseWriter<streaming>::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw TiFlashException(
            "Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    if (records_per_chunk == -1)
    {
        current_records_num = 0;
        if (block.rows() > 0)
        {
            chunk_codec_stream->encode(block, 0, block.rows());
            encodeChunkToDAGResponse();
        }
    }
    else
    {
        size_t rows = block.rows();
        for (size_t row_index = 0; row_index < rows;)
        {
            if (current_records_num >= records_per_chunk)
            {
                encodeChunkToDAGResponse();
            }
            const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
            chunk_codec_stream->encode(block, row_index, upper);
            current_records_num += (upper - row_index);
            row_index = upper;
        }
    }
}

template class DAGResponseWriter<false>;
template class DAGResponseWriter<true>;

} // namespace DB
