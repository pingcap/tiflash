// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Interpreters/AggregationCommon.h>
#include <iostream>

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
    DAGContext & dag_context_,
    uint32_t fine_grained_shuffle_stream_count_,
    Int64 fine_grained_shuffle_batch_size_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , exchange_type(exchange_type_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    switch (dag_context.encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeChunk:
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeCHBlock:
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    }
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        if (canUseFineGrainedShuffle())
            batchWriteFineGrainedShuffle<true>();
        else
            batchWrite<true>();
    }
    else
    {
        if (canUseFineGrainedShuffle())
            batchWriteFineGrainedShuffle<false>();
        else
            batchWrite<false>();
    }
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
{
    if (block.columns() != dag_context.result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (canUseFineGrainedShuffle())
    {
        if (static_cast<Int64>(rows_in_blocks) >= fine_grained_shuffle_batch_size)
            batchWriteFineGrainedShuffle<false>();
    }
    else
    {
        if (static_cast<Int64>(rows_in_blocks) > (dag_context.encode_type == tipb::EncodeType::TypeCHBlock ? batch_send_min_limit : records_per_chunk - 1))
            batchWrite<false>();
    }
}

template <class StreamWriterPtr>
bool StreamingDAGResponseWriter<StreamWriterPtr>::canUseFineGrainedShuffle() const
{
    // Fine grained shuffle will only be enabled when exchange_type is Hash.
    return enableFineGrainedShuffle(fine_grained_shuffle_stream_count) && exchange_type == tipb::ExchangeType::Hash;
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::encodeThenWriteBlocks(
    const std::vector<Block> & input_blocks,
    tipb::SelectResponse & response) const
{
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
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
                // Fine grained shuffle is disabled, all packet need to be handled by receiver's first stream.
                packet.add_stream_ids(0);
                chunk_codec_stream->clear();
            }
            writer->write(packet);
        }
        else /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        {
            response.set_encode_type(dag_context.encode_type);
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
        response.set_encode_type(dag_context.encode_type);
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

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::handleExecSummary(
                       const std::vector<Block> & input_blocks,
                       std::vector<mpp::MPPDataPacket> & packet,
                       tipb::SelectResponse & response) const
{
    if constexpr (send_exec_summary_at_last)
    {
        /// Sending the response to only one node, default the first one.
        serializeToPacket(packet[0], response);
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
    }
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::writePackets(const std::vector<size_t> & responses_row_count,
                                                               std::vector<mpp::MPPDataPacket> & packets) const
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(packets[part_id], part_id);
        }
        else
        {
            if (responses_row_count[part_id] > 0)
                writer->write(packets[part_id], part_id);
        }
    }
}

void initBlocks(Block & input_block,
                std::vector<MutableColumns> & dest_tbl_cols,
                uint32_t bucket_num)
{
    for (size_t i = 0; i < input_block.columns(); ++i)
    {
        if (ColumnPtr converted = input_block.getByPosition(i).column->convertToFullColumnIfConst())
        {
            input_block.getByPosition(i).column = converted;
        }
    }

    for (uint32_t i = 0; i < bucket_num; ++i)
    {
        dest_tbl_cols[i] = input_block.cloneEmptyColumns();
    }
}

inline void initInputBlocks(std::vector<Block> & input_blocks)
{
    for (auto & input_block : input_blocks)
    {
        for (size_t i = 0; i < input_block.columns(); ++i)
        {
            if (ColumnPtr converted = input_block.getByPosition(i).column->convertToFullColumnIfConst())
                input_block.getByPosition(i).column = converted;
        }
    }
}

inline void initDestColumns(const Block & input_block, std::vector<MutableColumns> & dest_tbl_cols)
{
    for (auto & cols : dest_tbl_cols)
    {
        cols = input_block.cloneEmptyColumns();
    }
}

void computeHash(const Block & input_block,
                 uint32_t bucket_num,
                 const TiDB::TiDBCollators & collators,
                 std::vector<String> & partition_key_containers,
                 const std::vector<Int64> & partition_col_ids,
                 std::vector<std::vector<MutableColumnPtr>> & result_columns)
{
    size_t rows = input_block.rows();
    WeakHash32 hash(rows);

    // get hash values by all partition key columns
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        input_block.getByPosition(partition_col_ids[i]).column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }

    const auto & hash_data = hash.getData();

    // partition each row
    IColumn::Selector selector(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        /// Row from interval [(2^32 / bucket_num) * i, (2^32 / bucket_num) * (i + 1)) goes to bucket with number i.
        selector[row] = hash_data[row]; /// [0, 2^32)
        selector[row] *= bucket_num; /// [0, bucket_num * 2^32), selector stores 64 bit values.
        selector[row] >>= 32u; /// [0, bucket_num)
    }

    for (size_t col_id = 0; col_id < input_block.columns(); ++col_id)
    {
        // Scatter columns to different partitions
        std::vector<MutableColumnPtr> part_columns = input_block.getByPosition(col_id).column->scatter(bucket_num, selector);
        assert(part_columns.size() == bucket_num);
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
        {
            result_columns[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
        }
    }
}

/// hash exchanging data among only TiFlash nodes.
template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::partitionAndEncodeThenWriteBlocks(
    std::vector<Block> & input_blocks,
    tipb::SelectResponse & response) const
{
    std::vector<mpp::MPPDataPacket> packet(partition_num);
    std::vector<size_t> responses_row_count(partition_num);
    handleExecSummary<send_exec_summary_at_last>(input_blocks, packet, response);
    if (input_blocks.empty())
        return;

    initInputBlocks(input_blocks);
    Block dest_block = input_blocks[0].cloneEmpty();
    std::vector<String> partition_key_containers(collators.size());
    for (const auto & block : input_blocks)
    {
        std::vector<MutableColumns> dest_tbl_cols(partition_num);
        initDestColumns(block, dest_tbl_cols);

        computeHash(block, partition_num, collators, partition_key_containers, partition_col_ids, dest_tbl_cols);

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
            responses_row_count[part_id] += dest_block.rows();
            chunk_codec_stream->encode(dest_block, 0, dest_block.rows());
            packet[part_id].add_chunks(chunk_codec_stream->getString());
            // Fine grained shuffle is disabled, so all packet will be handled by receiver's first stream.
            packet[part_id].add_stream_ids(0);
            chunk_codec_stream->clear();
        }
    }

    writePackets<send_exec_summary_at_last>(responses_row_count, packet);
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr>::batchWriteFineGrainedShuffle()
{
    assert(exchange_type == tipb::ExchangeType::Hash);

    tipb::SelectResponse response;
    if constexpr (send_exec_summary_at_last)
        addExecuteSummaries(response, !dag_context.isMPPTask() || dag_context.isRootMPPTask());

    std::vector<mpp::MPPDataPacket> packet(partition_num);
    std::vector<size_t> responses_row_count(partition_num, 0);

    // assert(fine_grained_shuffle_stream_count < 8192);
    uint32_t bucket_num = partition_num * fine_grained_shuffle_stream_count;
    handleExecSummary<send_exec_summary_at_last>(blocks, packet, response);
    if (!blocks.empty())
    {
        std::vector<MutableColumns> final_dest_tbl_columns(bucket_num);
        initInputBlocks(blocks);
        initDestColumns(blocks[0], final_dest_tbl_columns);

        for (const auto & block : blocks)
        {
            std::vector<String> partition_key_containers(collators.size());
            std::vector<MutableColumns> dest_tbl_columns(bucket_num);
            initDestColumns(block, dest_tbl_columns);
            computeHash(block, bucket_num, collators, partition_key_containers, partition_col_ids, dest_tbl_columns);
            for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
            {
                for (size_t col_id = 0; col_id < block.columns(); ++col_id)
                {
                    const MutableColumnPtr & src_col = dest_tbl_columns[bucket_idx][col_id];
                    final_dest_tbl_columns[bucket_idx][col_id]->insertRangeFrom(*src_col, 0, src_col->size());
                }
            }
        }

        for (size_t bucket_idx = 0; bucket_idx < bucket_num; bucket_idx += fine_grained_shuffle_stream_count)
        {
            size_t part_id = bucket_idx / fine_grained_shuffle_stream_count;
            size_t row_count_per_part = 0;
            for (uint32_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
            {
                Block dest_block = blocks[0].cloneEmpty();
                // For now we put all rows into one Block, may cause this Block too large.
                dest_block.setColumns(std::move(final_dest_tbl_columns[bucket_idx + stream_idx]));
                row_count_per_part += dest_block.rows();

                chunk_codec_stream->encode(dest_block, 0, dest_block.rows());
                packet[part_id].add_chunks(chunk_codec_stream->getString());
                packet[part_id].add_stream_ids(stream_idx);
                chunk_codec_stream->clear();
            }
            responses_row_count[part_id] = row_count_per_part;
        }
    }

    writePackets<send_exec_summary_at_last>(responses_row_count, packet);

    blocks.clear();
    rows_in_blocks = 0;
}

template class StreamingDAGResponseWriter<StreamWriterPtr>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr>;

} // namespace DB
