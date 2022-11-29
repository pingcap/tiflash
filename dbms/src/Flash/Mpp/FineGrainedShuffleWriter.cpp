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
template <class ExchangeWriterPtr>
FineGrainedShuffleWriter<ExchangeWriterPtr>::FineGrainedShuffleWriter(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    DAGContext & dag_context_,
    uint64_t fine_grained_shuffle_stream_count_,
    UInt64 fine_grained_shuffle_batch_size_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
    , batch_send_row_limit(fine_grained_shuffle_batch_size * fine_grained_shuffle_stream_count)
    , hash(0)
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::prepare(const Block & sample_block)
{
    /// Initialize header block, use column type to create new empty column to handle potential null column cases
    const auto & column_with_type_and_names = sample_block.getColumnsWithTypeAndName();
    for (const auto & column : column_with_type_and_names)
    {
        MutableColumnPtr empty_column = column.type->createColumn();
        ColumnWithTypeAndName new_column(std::move(empty_column), column.type, column.name);
        header.insert(new_column);
    }
    num_columns = header.columns();
    // fine_grained_shuffle_stream_count is in (0, 1024], and partition_num is uint16_t, so will not overflow.
    num_bucket = partition_num * fine_grained_shuffle_stream_count;
    partition_key_containers_for_reuse.resize(collators.size());
    initScatterColumns();
    prepared = true;
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        batchWriteFineGrainedShuffle();
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(prepared, "FineGrainedShuffleWriter should be prepared before writing.");
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");

    size_t rows = block.rows();
    if (rows > 0)
    {
        rows_in_blocks += rows;
        blocks.push_back(block);
    }

    if (blocks.size() == fine_grained_shuffle_stream_count || static_cast<UInt64>(rows_in_blocks) >= batch_send_row_limit)
        batchWriteFineGrainedShuffle();
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::initScatterColumns()
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

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::batchWriteFineGrainedShuffle()
{
    auto tracked_packets = HashBaseWriterHelper::createPackets(partition_num);
    if (likely(!blocks.empty()))
    {
        assert(rows_in_blocks > 0);
        assert(fine_grained_shuffle_stream_count <= 1024);

        HashBaseWriterHelper::materializeBlocks(blocks);
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            HashBaseWriterHelper::scatterColumnsForFineGrainedShuffle(block, partition_col_ids, collators, partition_key_containers_for_reuse, partition_num, fine_grained_shuffle_stream_count, hash, selector, scattered);
            blocks.pop_back();
        }

        // serialize each partitioned block and write it to its destination
        size_t part_id = 0;
        for (size_t bucket_idx = 0; bucket_idx < num_bucket; bucket_idx += fine_grained_shuffle_stream_count, ++part_id)
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
                tracked_packets[part_id]->addChunk(chunk_codec_stream->getString());
                tracked_packets[part_id]->getPacket().add_stream_ids(stream_idx);
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
        rows_in_blocks = 0;
    }

    writePackets(tracked_packets);
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::writePackets(TrackedMppDataPacketPtrs & packets)
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        auto & packet = packets[part_id];
        assert(packet);
        if (likely(packet->getPacket().chunks_size() > 0))
            writer->partitionWrite(std::move(packet), part_id);
    }
}

template class FineGrainedShuffleWriter<MPPTunnelSetPtr>;

} // namespace DB
