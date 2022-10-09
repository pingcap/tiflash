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
    UInt64 fine_grained_shuffle_batch_size_,
    bool reuse_scattered_columns_flag_,
    int stream_id_,
    const String & req_id)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
    , reuse_scattered_columns_flag(reuse_scattered_columns_flag_)
    , hash(0)
    , stream_id(stream_id_)
    , log(Logger::get("StreamingDagResponseWriter", req_id))
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
void FineGrainedShuffleWriter<StreamWriterPtr>::write(const Block & block, bool finish)
{
    if (unlikely(finish))
    {
	LOG_FMT_INFO(log, "LastWrite {} cached blocks", cached_block_count);
    	{
    	    if (cached_block_count > 0) {
    	    //if (static_cast<UInt64>(rows_in_blocks) >= fine_grained_shuffle_batch_size)
    	    //if (static_cast<UInt64>(rows_in_blocks) >= 60000 * fine_grained_shuffle_stream_count)
	    	//LOG_FMT_INFO(log, "LastTime SendPacket {} {}", cached_block_count, blocks.size());
    	        batchWriteFineGrainedShuffle<false>();
    	        cached_block_count = 0;
    	    }
    	}
	return;
    }

    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
	cached_block_count++;
    }

    //if (cached_block_count == fine_grained_shuffle_stream_count || static_cast<UInt64>(rows_in_blocks) >= fine_grained_shuffle_batch_size * (fine_grained_shuffle_stream_count >> 1)) {
    if (cached_block_count == fine_grained_shuffle_stream_count) {
        batchWriteFineGrainedShuffle<false>();
	cached_block_count = 0;
    }
}

static inline size_t nextPowOfTwo(size_t n)
{
    size_t t = 1;
    while (t < n)
        t <<= 1;
    return t;
}

void computeHashForReuse(
    const Block & block,
    uint32_t num_bucket,
    uint32_t num_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    const std::vector<Int64> & partition_col_ids,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered)
{
    size_t num_rows = block.rows();
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
        /// Row from interval [(2^32 / num_bucket) * i, (2^32 / num_bucket) * (i + 1)) goes to bucket with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= num_bucket; /// [0, num_bucket * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, num_bucket)
    }

    // partition
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector);
    }
}

template <class StreamWriterPtr>
void FineGrainedShuffleWriter<StreamWriterPtr>::resetScatterColumns()
{
    scattered.resize(num_columns);
    for (size_t col_id = 0; col_id < num_columns; ++col_id)
    {
        auto & column = header.getByPosition(col_id).column;

        scattered[col_id].reserve(num_bucket);
        for (size_t chunk_id = 0; chunk_id < num_bucket; ++chunk_id)
        {
            scattered[col_id].emplace_back(column->cloneEmpty());
            scattered[col_id][chunk_id]->reserve(1024);
        }
    }
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

	if (reuse_scattered_columns_flag)
	{
            if (!inited)
            {
                header = blocks[0].cloneEmpty();
                num_columns = header.columns();
                num_bucket = bucket_num;
                partition_key_containers_for_reuse.resize(collators.size());
                resetScatterColumns();
                inited = true;
            }

            for (const auto & block : blocks)
            {
                computeHashForReuse(block, num_bucket, num_columns, collators, partition_key_containers_for_reuse, partition_col_ids, hash, selector, scattered);
            }

            // serialize each partitioned block and write it to its destination
            // For i-th stream_count buckets, send to i-th tiflash node.
            size_t part_id = 0;
            for (size_t bucket_idx = 0; bucket_idx < bucket_num; bucket_idx += fine_grained_shuffle_stream_count, ++part_id)
            {
                for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
                {
                    // assemble scatter columns into a block
                    MutableColumns columns;
                    columns.reserve(num_columns);
                    for (size_t col_id = 0; col_id < num_columns; ++col_id)
                        columns.emplace_back(std::move(scattered[col_id][bucket_idx + stream_idx]));
                    auto block = header.cloneWithColumns(std::move(columns));
                    // encode into packet
                    chunk_codec_stream->encode(block, 0, block.rows());
                    tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
                    tracked_packets[part_id].packet.add_stream_ids(stream_idx);
                    chunk_codec_stream->clear();

                    // disassemble the block back to scatter columns
                    columns = block.mutateColumns();
                    for (size_t col_id = 0; col_id < num_columns; ++col_id)
                    {
                        columns[col_id]->popBack(columns[col_id]->size()); // clear column
                        scattered[col_id][bucket_idx + stream_idx] = std::move(columns[col_id]);
                    }
		}
	    }
	    blocks.clear();
	}
	else
	{
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
    }

    if (stream_id == 1)
    {
	LOG_FMT_INFO(log, "Stream1 writing packets!");
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
