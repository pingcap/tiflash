#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <class StreamWriterPtr>
StreamingDAGResponseWriter<StreamWriterPtr>::StreamingDAGResponseWriter(StreamWriterPtr writer_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_,
    bool collect_execute_summary_, bool return_executor_id_)
    : DAGResponseWriter(records_per_chunk_, encode_type_, result_field_types_, dag_context_, collect_execute_summary_, return_executor_id_),
      writer(writer_),
      thread_pool(dag_context.final_concurency)
{
    rows_in_blocks = 0;
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::ScheduleEncodeTask()
{
    tipb::SelectResponse response;
    addExecuteSummaries(response);
    thread_pool.schedule(getEncodeTask(blocks, response));
    blocks.clear();
    rows_in_blocks = 0;
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::finishWrite()
{
    if (rows_in_blocks > 0)
    {
        ScheduleEncodeTask();
    }
    // wait all job finishes.
    thread_pool.wait();
}

template <class StreamWriterPtr>
ThreadPool::Job StreamingDAGResponseWriter<StreamWriterPtr>::getEncodeTask(
    std::vector<Block> & input_blocks, tipb::SelectResponse & response) const
{
    /// todo find a way to avoid copying input_blocks
    return [this, input_blocks, response]() mutable {
        std::unique_ptr<ChunkCodecStream> chunk_codec_stream = nullptr;
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
        }

        response.set_encode_type(encode_type);
        Int64 current_records_num = 0;
        if (records_per_chunk == -1)
        {
            for (auto & block : input_blocks)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
            }
            auto dag_chunk = response.add_chunks();
            dag_chunk->set_rows_data(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
            current_records_num = 0;
        }
        else
        {
            for (auto & block : input_blocks)
            {
                size_t rows = block.rows();
                for (size_t row_index = 0; row_index < rows;)
                {
                    if (current_records_num >= records_per_chunk)
                    {
                        auto dag_chunk = response.add_chunks();
                        dag_chunk->set_rows_data(chunk_codec_stream->getString());
                        chunk_codec_stream->clear();
                        current_records_num = 0;
                    }
                    const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
                    chunk_codec_stream->encode(block, row_index, upper);
                    current_records_num += (upper - row_index);
                    row_index = upper;
                }
            }
        }
        if (current_records_num > 0)
        {
            auto dag_chunk = response.add_chunks();
            dag_chunk->set_rows_data(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }

        std::string select_resp;
        response.SerializeToString(&select_resp);

        writer->write(select_resp);
    };
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    rows_in_blocks += block.rows();
    blocks.push_back(block);
    if ((Int64)rows_in_blocks > records_per_chunk)
    {
        ScheduleEncodeTask();
    }
}

template class StreamingDAGResponseWriter<StreamWriterPtr>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr>;

} // namespace DB
