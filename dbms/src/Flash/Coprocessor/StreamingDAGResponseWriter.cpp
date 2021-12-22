#include <Common/LogWithPrefix.h>
#include <Common/TiFlashException.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Interpreters/AggregationCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

inline void serializeToPacket(mpp::MPPDataPacket & packet, const tipb::SelectResponse & response)
{
    if (!response.SerializeToString(packet.mutable_data()))
        throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));
}

template <class StreamWriterPtr>
StreamingDAGResponseWriter<StreamWriterPtr>::StreamingDAGResponseWriter(
    StreamWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    tipb::ExchangeType exchange_type_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    tipb::EncodeType encode_type_,
    std::vector<tipb::FieldType> result_field_types_,
    DAGContext & dag_context_,
    const LogWithPrefixPtr & log_,
    Int64 num_streams_)
    : DAGResponseWriter(records_per_chunk_, encode_type_, result_field_types_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , exchange_type(exchange_type_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , num_streams(num_streams_)
    , hash(0)
{
    log = log_ != nullptr ? log_ : std::make_shared<LogWithPrefix>(&Poco::Logger::get("StreamingDAGResponseWriter"), "");

    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
        batchWrite<true>();
    else
        batchWrite<false>();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);

        if (exchange_type == tipb::ExchangeType::Hash)
            prePartition(blocks.back());
    }

    // if ((Int64)rows_in_blocks > (encode_type == tipb::EncodeType::TypeCHBlock ? batch_send_min_limit : records_per_chunk - 1))
    size_t batch_size = (encode_type == tipb::EncodeType::TypeCHBlock ? 100000 : records_per_chunk - 1);
    if (rows_in_blocks > batch_size)
        batchWrite<false>();
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::encodeThenWriteBlocks(
    const std::vector<Block> & input_blocks,
    tipb::SelectResponse & response) const
{
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

    if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        if (dag_context.isMPPTask()) /// broadcast data among TiFlash nodes in MPP
        {
            mpp::MPPDataPacket packet;
            if constexpr (send_exec_summary_at_last)
            {
                serializeToPacket(packet, response);
            }
            if (input_blocks.empty())
            {
                if constexpr (send_exec_summary_at_last)
                {
                    writer->write(packet);
                }
                return;
            }
            for (const auto & block : input_blocks)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
                packet.add_chunks(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
            writer->write(packet);
        }
        else /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        {
            response.set_encode_type(encode_type);
            if (input_blocks.empty())
            {
                if constexpr (send_exec_summary_at_last)
                {
                    writer->write(response);
                }
                return;
            }
            for (const auto & block : input_blocks)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
                auto * dag_chunk = response.add_chunks();
                dag_chunk->set_rows_data(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
            writer->write(response);
        }
    }
    else /// passthrough data to a TiDB node
    {
        response.set_encode_type(encode_type);
        if (input_blocks.empty())
        {
            if constexpr (send_exec_summary_at_last)
            {
                writer->write(response);
            }
            return;
        }

        Int64 current_records_num = 0;
        for (const auto & block : input_blocks)
        {
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows;)
            {
                if (current_records_num >= records_per_chunk)
                {
                    auto * dag_chunk = response.add_chunks();
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

        if (current_records_num > 0)
        {
            auto * dag_chunk = response.add_chunks();
            dag_chunk->set_rows_data(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        writer->write(response);
    }
}

static inline size_t nextPowOfTwo(size_t n)
{
    size_t t = 1;
    while (t < n)
        t <<= 1;
    return t;
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::resetScatterColumns()
{
    scattered.resize(num_columns);
    for (size_t col_id = 0; col_id < num_columns; ++col_id)
    {
        auto & column = header.getByPosition(col_id).column;

        scattered.clear();
        scattered[col_id].reserve(num_chunks);
        for (size_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id)
        {
            scattered[col_id].emplace_back(column->cloneEmpty());
            scattered[col_id][chunk_id]->reserve(initial_size_hint);
        }
    }
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::resetBlockCodec()
{
    codec->clear();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::prePartition(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (ColumnPtr converted = block.getByPosition(i).column->convertToFullColumnIfConst())
            block.getByPosition(i).column = converted;
    }

    size_t num_rows = block.rows();

    if (!inited)
    {
        header = block.cloneEmpty();
        num_columns = header.columns();
        num_chunks = num_streams * partition_num;
        partition_key_containers.resize(collators.size());

        resetScatterColumns();

        inited = true;
    }

    // compute hash values
    hash.getData().reserve(nextPowOfTwo(num_rows));
    hash.reset(num_rows);
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        const auto & column = block.getByPosition(partition_col_ids[i]).column;
        column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }

    // fill selector array with most significant bits of hash values
    selector.reserve(nextPowOfTwo(num_rows));
    selector.resize(num_rows);
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Row from interval [(2^32 / partition_num) * i, (2^32 / partition_num) * (i + 1)) goes to bucket with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= num_chunks; /// [0, partition_num * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, partition_num)
    }

    // partition
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector);
    }
}

/// hash exchanging data among only TiFlash nodes.
template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::partitionAndEncodeThenWriteBlocks(
    std::vector<Block> & input_blocks,
    tipb::SelectResponse & response)
{
    std::vector<mpp::MPPDataPacket> packet(partition_num);

    std::vector<size_t> responses_row_count(partition_num);
    for (auto i = 0; i < partition_num; ++i)
    {
        if constexpr (send_exec_summary_at_last)
        {
            /// Sending the response to only one node, default the first one.
            if (i == 0)
                serializeToPacket(packet[i], response);
        }
    }
    if (input_blocks.empty())
    {
        if constexpr (send_exec_summary_at_last)
        {
            for (auto part_id = 0; part_id < partition_num; ++part_id)
            {
                writer->write(packet[part_id], part_id);
            }
        }
        return;
    }

    // partition tuples in blocks
    // 1) compute partition id
    // 2) partition each row
    // 3) encode each chunk and send it
    // NOTE: 1) and 2) are already done in prePartition

    // encode chunks
    if (!codec)
    {
        if (encode_type == tipb::EncodeType::TypeDefault)
            codec = DefaultChunkCodec().newCodecStream(result_field_types);
        else if (encode_type == tipb::EncodeType::TypeChunk)
            codec = ArrowChunkCodec().newCodecStream(result_field_types);
        else if (encode_type == tipb::EncodeType::TypeCHBlock)
            codec = CHBlockChunkCodec().newCodecStream(result_field_types);
    }

    // serialize each partitioned block and write it to its destination
    for (size_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id)
    {
        // assemble scatter columns into a block
        MutableColumns columns;
        columns.reserve(num_columns);
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
            columns.emplace_back(std::move(scattered[col_id][chunk_id]));

        auto block = header.cloneWithColumns(std::move(columns));

        // encode into packet
        size_t part_id = chunk_id / num_streams;
        responses_row_count[part_id] += block.rows();

        codec->encode(block, 0, block.rows());
        packet[part_id].add_chunks(codec->getString()); // std::move?
        resetBlockCodec();

        // disassemble the block back to scatter columns
        columns = block.mutateColumns();
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
        {
            columns[col_id]->popBack(columns[col_id]->size()); // clear column
            scattered[col_id][chunk_id] = std::move(columns[col_id]);
        }
    }

    // send packets
    for (auto part_id = 0; part_id < partition_num; ++part_id)
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(packet[part_id], part_id);
        }
        else
        {
            if (responses_row_count[part_id] > 0)
                writer->write(packet[part_id], part_id);
        }
    }
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::batchWrite()
{
    tipb::SelectResponse response;
    if constexpr (send_exec_summary_at_last)
        addExecuteSummaries(response, !dag_context.isMPPTask() || dag_context.isRootMPPTask());
    if (exchange_type == tipb::ExchangeType::Hash)
    {
        partitionAndEncodeThenWriteBlocks<send_exec_summary_at_last>(blocks, response);
    }
    else
    {
        encodeThenWriteBlocks<send_exec_summary_at_last>(blocks, response);
    }
    blocks.clear();
    rows_in_blocks = 0;
}

template class StreamingDAGResponseWriter<StreamWriterPtr>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr>;

} // namespace DB
