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
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/AsyncMPPTunnelWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Coprocessor/DAGContext.h>

#include <iostream>

namespace DB
{
namespace
{
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
}

AsyncMPPTunnelWriter::AsyncMPPTunnelWriter(
    MPPTunnelSetPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    tipb::ExchangeType exchange_type_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_)
    : records_per_chunk(records_per_chunk_)
    , dag_context(dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , exchange_type(exchange_type_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
{
    RUNTIME_CHECK(dag_context.isMPPTask(), dag_context.isMPPTask());

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

bool AsyncMPPTunnelWriter::isReady()
{
    return true;
}

void AsyncMPPTunnelWriter::finishWrite()
{
    batchWrite();
}

void AsyncMPPTunnelWriter::write(Block && block)
{
    if (block.columns() != dag_context.result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.emplace_back(std::move(block));
        if (static_cast<Int64>(rows_in_blocks) > (dag_context.encode_type == tipb::EncodeType::TypeCHBlock ? batch_send_min_limit : records_per_chunk - 1))
            batchWrite();
    }
}

void AsyncMPPTunnelWriter::encodeThenWriteBlocks(const std::vector<Block> & input_blocks) const
{
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        if (input_blocks.empty())
            return;
        TrackedMppDataPacket tracked_packet(current_memory_tracker);
        for (const auto & block : input_blocks)
        {
            chunk_codec_stream->encode(block, 0, block.rows());
            tracked_packet.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        writer->write(tracked_packet.getPacket());
    }
    else /// passthrough data to a TiDB node
    {
        if (input_blocks.empty())
            return;

        TrackedSelectResp response;
        response.setEncodeType(dag_context.encode_type);

        Int64 current_records_num = 0;
        for (const auto & block : input_blocks)
        {
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows;)
            {
                if (current_records_num >= records_per_chunk)
                {
                    response.addChunk(chunk_codec_stream->getString());
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
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        writer->write(response.getResponse());
    }
}


void AsyncMPPTunnelWriter::batchWrite()
{
    if (exchange_type == tipb::ExchangeType::Hash)
    {
        partitionAndEncodeThenWriteBlocks(blocks);
    }
    else
    {
        encodeThenWriteBlocks(blocks);
    }
    blocks.clear();
    rows_in_blocks = 0;
}


void AsyncMPPTunnelWriter::writePackets(
    const std::vector<size_t> & responses_row_count,
    std::vector<TrackedMppDataPacket> & packets) const
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        if (responses_row_count[part_id] > 0)
            writer->write(packets[part_id].getPacket(), part_id);
    }
}

void AsyncMPPTunnelWriter::partitionAndEncodeThenWriteBlocks(std::vector<Block> & input_blocks) const
{
    if (input_blocks.empty())
        return;

    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);
    std::vector<size_t> responses_row_count(partition_num);

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
            tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    writePackets(responses_row_count, tracked_packets);
}
} // namespace DB
