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
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/HashParitionWriter.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <iostream>

namespace DB
{
template <class StreamWriterPtr>
HashParitionWriter<StreamWriterPtr>::HashParitionWriter(
    StreamWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class StreamWriterPtr>
void HashParitionWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        batchWrite<true>();
    }
    else
    {
        batchWrite<false>();
    }
}

template <class StreamWriterPtr>
void HashParitionWriter<StreamWriterPtr>::write(const Block & block)
{
    if (block.columns() != dag_context.result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        batchWrite<false>();
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashParitionWriter<StreamWriterPtr>::batchWrite()
{
    partitionAndEncodeThenWriteBlocks<send_exec_summary_at_last>(blocks);
    blocks.clear();
    rows_in_blocks = 0;
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashParitionWriter<StreamWriterPtr>::handleExecSummary(
    const std::vector<Block> & input_blocks,
    std::vector<TrackedMppDataPacket> & packets)
{
    if constexpr (send_exec_summary_at_last)
    {
        TrackedSelectResp response;
        addExecuteSummaries(response.getResponse(), /*delta_mode=*/false);

        /// Sending the response to only one node, default the first one.
        packets[0].serializeByResponse(response.getResponse());

        // No need to send data when blocks are not empty,
        // because exec_summary will be sent together with blocks.
        if (input_blocks.empty())
        {
            for (auto part_id = 0; part_id < partition_num; ++part_id)
            {
                writer->write(packets[part_id].getPacket(), part_id);
            }
        }
    }
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashParitionWriter<StreamWriterPtr>::writePackets(
    const std::vector<size_t> & responses_row_count,
    std::vector<TrackedMppDataPacket> & packets)
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(packets[part_id].getPacket(), part_id);
        }
        else
        {
            if (responses_row_count[part_id] > 0)
                writer->write(packets[part_id].getPacket(), part_id);
        }
    }
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashParitionWriter<StreamWriterPtr>::partitionAndEncodeThenWriteBlocks(std::vector<Block> & input_blocks)
{
    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);
    handleExecSummary<send_exec_summary_at_last>(input_blocks, tracked_packets);
    if (input_blocks.empty())
        return;

    std::vector<size_t> responses_row_count(partition_num);
    HashBaseWriterHelper::initInputBlocks(input_blocks);
    Block dest_block = input_blocks[0].cloneEmpty();
    std::vector<String> partition_key_containers(collators.size());
    for (const auto & block : input_blocks)
    {
        std::vector<MutableColumns> dest_tbl_cols(partition_num);
        HashBaseWriterHelper::initDestColumns(block, dest_tbl_cols);

        HashBaseWriterHelper::computeHash(block, partition_num, collators, partition_key_containers, partition_col_ids, dest_tbl_cols);

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
            responses_row_count[part_id] += dest_block.rows();
            chunk_codec_stream->encode(dest_block, 0, dest_block.rows());
            tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    writePackets<send_exec_summary_at_last>(responses_row_count, tracked_packets);
}

template class HashParitionWriter<MPPTunnelSetPtr>;

} // namespace DB
