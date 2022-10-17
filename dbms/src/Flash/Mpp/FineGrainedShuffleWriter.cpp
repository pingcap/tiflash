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
#include <Flash/Mpp/FineGrainedShuffleWriter.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class StreamWriterPtr>
FineGrainedShuffleWriter<StreamWriterPtr>::FineGrainedShuffleWriter(
    StreamWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_,
    uint64_t fine_grained_shuffle_stream_count_,
    UInt64 fine_grained_shuffle_batch_size_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class StreamWriterPtr>
void FineGrainedShuffleWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        batchWriteFineGrainedShuffle<true>();
    }
    else
    {
        batchWriteFineGrainedShuffle<false>();
    }
}

template <class StreamWriterPtr>
void FineGrainedShuffleWriter<StreamWriterPtr>::write(const Block & block)
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

    if (static_cast<UInt64>(rows_in_blocks) >= fine_grained_shuffle_batch_size)
        batchWriteFineGrainedShuffle<false>();
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void FineGrainedShuffleWriter<StreamWriterPtr>::batchWriteFineGrainedShuffle()
{
    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);

    if (!blocks.empty())
    {
        assert(rows_in_blocks > 0);
        assert(fine_grained_shuffle_stream_count <= 1024);

        // fine_grained_shuffle_stream_count is in (0, 1024], and partition_num is uint16_t, so will not overflow.
        uint32_t bucket_num = partition_num * fine_grained_shuffle_stream_count;

        HashBaseWriterHelper::materializeBlocks(blocks);
        auto final_dest_tbl_columns = HashBaseWriterHelper::createDestColumns(blocks[0], bucket_num);
        Block dest_block = blocks[0].cloneEmpty();

        // Hash partition input_blocks into bucket_num.
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            size_t columns = block.columns();
            std::vector<String> partition_key_containers(collators.size());
            auto dest_tbl_columns = HashBaseWriterHelper::createDestColumns(block, bucket_num);
            HashBaseWriterHelper::computeHash(block, bucket_num, collators, partition_key_containers, partition_col_ids, dest_tbl_columns);
            blocks.pop_back();

            for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
            {
                for (size_t col_id = 0; col_id < columns; ++col_id)
                {
                    const MutableColumnPtr & src_col = dest_tbl_columns[bucket_idx][col_id];
                    final_dest_tbl_columns[bucket_idx][col_id]->insertRangeFrom(*src_col, 0, src_col->size());
                }
            }
        }
        assert(blocks.empty());
        rows_in_blocks = 0;

        // For i-th stream_count buckets, send to i-th tiflash node.
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; bucket_idx += fine_grained_shuffle_stream_count)
        {
            size_t part_id = bucket_idx / fine_grained_shuffle_stream_count; // NOLINT(clang-analyzer-core.DivideZero)
            for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
            {
                // For now we put all rows into one Block, may cause this Block too large.
                dest_block.setColumns(std::move(final_dest_tbl_columns[bucket_idx + stream_idx]));
                size_t dest_block_rows = dest_block.rows();
                if (dest_block_rows > 0)
                {
                    chunk_codec_stream->encode(dest_block, 0, dest_block_rows);
                    tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                    tracked_packets[part_id].packet.add_stream_ids(stream_idx);
                }
            }
        }
    }

    writePackets<send_exec_summary_at_last>(tracked_packets);
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void FineGrainedShuffleWriter<StreamWriterPtr>::writePackets(std::vector<TrackedMppDataPacket> & packets)
{
    size_t part_id = 0;

    if constexpr (send_exec_summary_at_last)
    {
        tipb::SelectResponse response;
        addExecuteSummaries(response, /*delta_mode=*/false);
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

template class FineGrainedShuffleWriter<MPPTunnelSetPtr>;

} // namespace DB
