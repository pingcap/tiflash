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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/HashParitionWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class StreamWriterPtr>
HashPartitionWriter<StreamWriterPtr>::HashPartitionWriter(
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
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class StreamWriterPtr>
void HashPartitionWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        partitionAndEncodeThenWriteBlocks<true>();
    }
    else
    {
        partitionAndEncodeThenWriteBlocks<false>();
    }
}

template <class StreamWriterPtr>
void HashPartitionWriter<StreamWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        partitionAndEncodeThenWriteBlocks<false>();
}

template <class StreamWriterPtr>
void HashPartitionWriter<StreamWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        partitionAndEncodeThenWriteBlocks<false>();
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashPartitionWriter<StreamWriterPtr>::partitionAndEncodeThenWriteBlocks()
{
    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);

    if (!blocks.empty())
    {
        assert(rows_in_blocks > 0);

        HashBaseWriterHelper::materializeBlocks(blocks);
        Block dest_block = blocks[0].cloneEmpty();
        std::vector<String> partition_key_containers(collators.size());

        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            auto dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
            HashBaseWriterHelper::scatterColumns(block, partition_num, collators, partition_key_containers, partition_col_ids, dest_tbl_cols);
            blocks.pop_back();

            for (size_t part_id = 0; part_id < partition_num; ++part_id)
            {
                dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
                size_t dest_block_rows = dest_block.rows();
                if (dest_block_rows > 0)
                {
                    chunk_codec_stream->encode(dest_block, 0, dest_block_rows);
                    tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                }
            }
        }
        assert(blocks.empty());
        rows_in_blocks = 0;
    }

    writePackets<send_exec_summary_at_last>(tracked_packets);
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void HashPartitionWriter<StreamWriterPtr>::writePackets(std::vector<TrackedMppDataPacket> & packets)
{
    size_t part_id = 0;

    if constexpr (send_exec_summary_at_last)
    {
        tipb::SelectResponse response;
        summary_collector.addExecuteSummaries(response, /*delta_mode=*/false);
        /// Sending the response to only one node, default the first one.
        assert(!packets.empty());
        packets[0].serializeByResponse(response);
        writer->write(packets[0].getPacket(), 0);
        part_id = 1;
    }

    for (; part_id < packets.size(); ++part_id)
    {
        auto & packet = packets[part_id].getPacket();
        if (packet.chunks_size() > 0)
            writer->write(packet, part_id);
    }
}

template class HashPartitionWriter<MPPTunnelSetPtr>;

} // namespace DB
