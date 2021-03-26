#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Interpreters/AggregationCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <class StreamWriterPtr>
StreamingDAGResponseWriter<StreamWriterPtr>::StreamingDAGResponseWriter(StreamWriterPtr writer_, std::vector<Int64> partition_col_ids_,
    tipb::ExchangeType exchange_type_, Int64 records_per_chunk_, tipb::EncodeType encode_type_,
    std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, encode_type_, result_field_types_, dag_context_),
      exchange_type(exchange_type_),
      writer(writer_),
      partition_col_ids(std::move(partition_col_ids_)),
      thread_pool(dag_context.final_concurrency)
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::ScheduleEncodeTask()
{
    tipb::SelectResponse response;
    addExecuteSummaries(response);
    if (exchange_type == tipb::ExchangeType::Hash)
    {
        thread_pool.schedule(getEncodePartitionTask(blocks, response));
    }
    else
    {
        thread_pool.schedule(getEncodeTask(blocks, response));
    }
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

        writer->write(response);
    };
}

template <class StreamWriterPtr>
ThreadPool::Job StreamingDAGResponseWriter<StreamWriterPtr>::getEncodePartitionTask(
    std::vector<Block> & input_blocks, tipb::SelectResponse & response) const
{
    /// todo find a way to avoid copying input_blocks
    return [this, input_blocks, response]() mutable {
        std::vector<std::unique_ptr<ChunkCodecStream>> chunk_codec_stream(partition_num);
        std::vector<tipb::SelectResponse> responses(partition_num);
        for (auto i = 0; i < partition_num; ++i)
        {
            if (encode_type == tipb::EncodeType::TypeDefault)
            {
                chunk_codec_stream[i] = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
            }
            else if (encode_type == tipb::EncodeType::TypeChunk)
            {
                chunk_codec_stream[i] = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
            }
            else if (encode_type == tipb::EncodeType::TypeCHBlock)
            {
                chunk_codec_stream[i] = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
            }
            responses[i] = response;
            responses[i].set_encode_type(encode_type);
        }

        // partition tuples in blocks
        // 1) compute partition id
        // 2) partition each row
        // 3) encode each chunk and send it
        for (auto & block : input_blocks)
        {
            ColumnRawPtrs key_col_ptrs;
            std::vector<Block> dest_blocks(partition_num);
            std::vector<MutableColumns> dest_tbl_cols(partition_num);

            for (size_t i = 0; i < block.columns(); ++i)
            {
                if (ColumnPtr converted = block.getByPosition(i).column->convertToFullColumnIfConst())
                {
                    block.getByPosition(i).column = converted;
                }
            }

            for (auto i = 0; i < partition_num; ++i)
            {
                dest_tbl_cols[i] = block.cloneEmptyColumns();
                dest_blocks[i] = block.cloneEmpty();
            }
            // get partition key column ids
            for (auto i : partition_col_ids)
            {
                key_col_ptrs.emplace_back(block.getByPosition(i).column.get());
            }
            // partition each row
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows; ++row_index)
            {
                // TODO: add specific collators
                UInt128 key = hash128(row_index, key_col_ptrs.size(), key_col_ptrs, TiDB::dummy_collators, TiDB::dummy_sort_key_contaners);
                auto part_id = (key.low % partition_num);
                // copy each field
                for (size_t col_id = 0; col_id < block.columns(); ++col_id)
                {
                    dest_tbl_cols[part_id][col_id]->insert(block.getByPosition(col_id).column->operator[](row_index));
                }
            }
            // serialize each partitioned block and write it to its destination
            for (auto part_id = 0; part_id < partition_num; ++part_id)
            {
                dest_blocks[part_id].setColumns(std::move(dest_tbl_cols[part_id]));
                chunk_codec_stream[part_id]->encode(dest_blocks[part_id], 0, dest_blocks[part_id].rows());
                auto dag_chunk = responses[part_id].add_chunks();
                dag_chunk->set_rows_data(chunk_codec_stream[part_id]->getString());
                chunk_codec_stream[part_id]->clear();
                writer->write(responses[part_id], part_id);
            }
        }
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
